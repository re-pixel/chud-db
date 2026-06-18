package file_writer

type FileWriterInterface interface {
	Write(data []byte, sectionEnd bool, size []byte) int
	Commit() error
	ResetFileWriter(name string)
}
