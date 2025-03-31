package main

import (
	"fmt"
	"log"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

// Student represents a sample data structure
type Student struct {
	Name    string  `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
	Age     int32   `parquet:"name=age, type=INT32"`
	ID      int64   `parquet:"name=id, type=INT64"`
	Weight  float32 `parquet:"name=weight, type=FLOAT"`
	GPA     float64 `parquet:"name=gpa, type=DOUBLE"`
	Active  bool    `parquet:"name=active, type=BOOLEAN"`
	Courses []string `parquet:"name=courses, type=MAP, convertedtype=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
}

func main() {
	if err := generateSampleParquet("sample.parquet", 10); err != nil {
		log.Fatal("Error generating sample parquet file:", err)
	}
	fmt.Println("Sample parquet file 'sample.parquet' generated successfully")
	fmt.Println("Run the viewer with: go run main.go sample.parquet")
}

func generateSampleParquet(filePath string, numRows int) error {
	// Create file writer
	fw, err := local.NewLocalFileWriter(filePath)
	if err != nil {
		return fmt.Errorf("cannot create local file writer: %v", err)
	}
	defer fw.Close()

	// Create parquet writer
	pw, err := writer.NewParquetWriter(fw, new(Student), 4)
	if err != nil {
		return fmt.Errorf("cannot create parquet writer: %v", err)
	}

	// Set compression
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	// Write sample data
	for i := 0; i < numRows; i++ {
		student := Student{
			Name:    fmt.Sprintf("Student %d", i+1),
			Age:     int32(18 + i%10),
			ID:      int64(1000 + i),
			Weight:  float32(50.5 + float32(i)*0.5),
			GPA:     float64(3.0 + float64(i%10)/10.0),
			Active:  i%2 == 0,
			Courses: []string{fmt.Sprintf("Course %d", i%5+1), fmt.Sprintf("Course %d", (i+2)%5+1)},
		}

		if err := pw.Write(student); err != nil {
			return fmt.Errorf("write error: %v", err)
		}
	}

	if err := pw.WriteStop(); err != nil {
		return fmt.Errorf("write stop error: %v", err)
	}

	return nil
}