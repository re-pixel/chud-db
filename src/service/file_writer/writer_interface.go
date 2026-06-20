package file_writer

type FileWriterInterface interface {
	Write(data []byte, sectionEnd bool, size []byte) int
	WriteRaw(data []byte) (int64, error)
	CurrentByteOffset() int64
	Commit() error
	ResetFileWriter(name string)
	GetLocation() string
}
