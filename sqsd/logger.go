package sqsd

type Logger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
}
