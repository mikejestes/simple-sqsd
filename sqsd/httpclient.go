package sqsd

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"

	"github.com/aws/aws-sdk-go/service/sqs"
)

type HTTPClient struct {
	httpClient        *http.Client
	url               string
	contentTypeHeader string
	hmacOptions       *HMACOptions
}

type HMACOptions struct {
	Signature  string
	Secret     string
	HTTPHeader string
}

type Result struct {
	StatusCode    int
	Successful    bool
	RetryAfterSec int64
}

func NewHTTPClient(httpClient *http.Client, url string, contentTypeHeader string, HMACOpts *HMACOptions) *HTTPClient {
	return &HTTPClient{
		httpClient:        httpClient,
		url:               url,
		contentTypeHeader: contentTypeHeader,
		hmacOptions:       HMACOpts,
	}
}

func (c *HTTPClient) Request(msg *sqs.Message) (*Result, error) {
	req, err := http.NewRequest("POST", c.url, bytes.NewBufferString(*msg.Body))
	if err != nil {
		return nil, fmt.Errorf("Error creating the HTTP request: %s", err)
	}

	// Add the X-Aws-Sqsd-Msgid header.
	req.Header.Add("X-Aws-Sqsd-Msgid", *msg.MessageId)

	// Add a X-Aws-Sqsd-Attr-* header for each attribute included on the SQS message.
	for k, v := range msg.MessageAttributes {
		req.Header.Add(fmt.Sprintf("X-Aws-Sqsd-Attr-%s", k), *v.StringValue)
	}

	// If there are HMAC options, add the hash as a header.
	if c.hmacOptions != nil {
		hash, err := makeHMAC(fmt.Sprintf("%s%s", c.hmacOptions.Signature, *msg.Body), c.hmacOptions.Secret)
		if err != nil {
			return nil, err
		}

		req.Header.Set(c.hmacOptions.HTTPHeader, hash)
	}

	// If there is a Content-Type header, add it as a header.
	if len(c.contentTypeHeader) > 0 {
		req.Header.Set("Content-Type", c.contentTypeHeader)
	}

	res, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	err = res.Body.Close()
	if err != nil {
		return nil, err
	}

	return &Result{
		StatusCode:    res.StatusCode,
		Successful:    res.StatusCode >= http.StatusOK || res.StatusCode <= http.StatusIMUsed,
		RetryAfterSec: getRetryAfterFromResponse(res),
	}, nil
}

func makeHMAC(signature string, secret string) (string, error) {
	mac := hmac.New(sha256.New, []byte(secret))

	_, err := mac.Write([]byte(signature))
	if err != nil {
		return "", fmt.Errorf("Error while writing HMAC: %s", err)
	}

	return hex.EncodeToString(mac.Sum(nil)), nil
}

func getRetryAfterFromResponse(res *http.Response) int64 {
	retryAfter := res.Header.Get("Retry-After")
	if len(retryAfter) == 0 {
		return 0
	}

	seconds, err := strconv.ParseInt(retryAfter, 10, 0)
	if err != nil {
		return 0
	}

	return seconds
}
