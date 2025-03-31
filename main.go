package main

import (
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"

	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/bubbles/textinput"
	"github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
)

// ViewState represents the current view of the application
type ViewState int

const (
	InputView ViewState = iota
	FileSelectView
	TableView
)

// Model represents the application state
type Model struct {
	table            table.Model
	textInput        textinput.Model
	filePath         string
	error            error
	columns          []string
	rows             [][]string
	maxRows          int
	isLoading        bool
	viewState        ViewState
	parquetFiles     []string
	selectedFile     int
	currentRowOffset int                   // Current offset in the parquet file
	totalRows        int                   // Total number of rows in the parquet file
	parquetReader    *reader.ParquetReader // Reference to the parquet reader
	parquetFile      source.ParquetFile    // Reference to the parquet file
}

// Init initializes the application
func (m Model) Init() tea.Cmd {
	if m.filePath != "" {
		return loadParquetData(m.filePath, m.maxRows)
	}
	return tea.Batch(textinput.Blink, findParquetFiles())
}

// Update handles events and updates the model
func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case parquetFilesMsg:
		m.parquetFiles = msg.files
		if len(m.parquetFiles) > 0 {
			m.viewState = FileSelectView
		}
		return m, nil

	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+d":
			return m, tea.Quit
		case "q", "esc":
			// Return to file selection view when in table view
			if m.viewState == TableView {
				// Clean up parquet reader and file
				if m.parquetReader != nil {
					m.parquetReader.ReadStop()
					m.parquetReader = nil
				}
				if m.parquetFile != nil {
					err := m.parquetFile.Close()
					if err != nil {
						return nil, nil
					}
					m.parquetFile = nil
				}
				m.viewState = FileSelectView
				return m, findParquetFiles()
			}
		case "enter":
			if m.viewState == InputView && m.textInput.Value() != "" {
				m.filePath = m.textInput.Value()
				m.isLoading = true
				m.viewState = TableView
				return m, loadParquetData(m.filePath, m.maxRows)
			} else if m.viewState == FileSelectView && len(m.parquetFiles) > 0 {
				m.filePath = m.parquetFiles[m.selectedFile]
				m.isLoading = true
				m.viewState = TableView
				return m, loadParquetData(m.filePath, m.maxRows)
			}
		case "up", "k":
			if m.viewState == FileSelectView {
				m.selectedFile--
				if m.selectedFile < 0 {
					m.selectedFile = len(m.parquetFiles) - 1
				}
			}
		case "down", "j":
			if m.viewState == FileSelectView {
				m.selectedFile++
				if m.selectedFile >= len(m.parquetFiles) {
					m.selectedFile = 0
				}
			}
		}

	case parquetDataMsg:
		m.isLoading = false
		if msg.err != nil {
			m.error = msg.err
			return m, nil
		}

		m.columns = msg.columns
		m.rows = msg.rows
		m.totalRows = msg.totalRows
		m.parquetReader = msg.parquetReader
		m.parquetFile = msg.parquetFile
		m.currentRowOffset = len(msg.rows)

		// Create table columns
		var columns []table.Column
		for _, col := range m.columns {
			columns = append(columns, table.Column{
				Title: col,
				Width: 20,
			})
		}

		// Create table rows
		var rows []table.Row
		for _, row := range m.rows {
			rows = append(rows, row)
		}

		// Initialize table
		t := table.New(
			table.WithColumns(columns),
			table.WithRows(rows),
			table.WithFocused(true),
			table.WithHeight(10),
		)

		s := table.DefaultStyles()
		s.Header = s.Header.BorderStyle(lipgloss.NormalBorder()).BorderForeground(lipgloss.Color("240")).BorderBottom(true).Bold(true)
		s.Selected = s.Selected.Foreground(lipgloss.Color("229")).Background(lipgloss.Color("57")).Bold(true)
		t.SetStyles(s)

		m.table = t

	case moreRowsMsg:
		if msg.err != nil {
			m.error = msg.err
			return m, nil
		}

		// Add the new row to the bottom and remove the first row
		m.rows = append(m.rows[1:], msg.newRow)
		m.currentRowOffset++

		// Update the table rows
		var rows []table.Row
		for _, row := range m.rows {
			rows = append(rows, row)
		}

		// Keep the cursor at the bottom row
		cursor := len(rows) - 1
		m.table.SetRows(rows)
		m.table.SetCursor(cursor)

		return m, nil
	}

	// Handle input updates when in input view
	if m.viewState == InputView {
		var cmd tea.Cmd
		m.textInput, cmd = m.textInput.Update(msg)
		return m, cmd
	}

	// Handle table updates when in table view
	if m.viewState == TableView {
		var cmd tea.Cmd
		oldCursor := m.table.Cursor()
		m.table, cmd = m.table.Update(msg)
		newCursor := m.table.Cursor()

		// Check if we've moved to the last row and need to load more data
		if newCursor > oldCursor && newCursor == len(m.rows)-1 && m.currentRowOffset+len(m.rows) < m.totalRows {
			return m, loadMoreRows(m)
		}
		return m, cmd
	}

	return m, nil
}

// View renders the application UI
func (m Model) View() string {
	if m.viewState == InputView {
		return fmt.Sprintf(
			"\n  Enter path to parquet file:\n\n  %s\n\n  (Press Enter to load file, Ctrl+D to quit)\n",
			m.textInput.View(),
		)
	}

	if m.viewState == FileSelectView {
		if len(m.parquetFiles) == 0 {
			return "\n  No parquet files found in current directory.\n\n  Enter path to parquet file:\n\n  " +
				m.textInput.View() + "\n\n  (Press Enter to load file, Ctrl+D to quit)\n"
		}

		s := "\n  Select a parquet file to view:\n\n"
		for i, file := range m.parquetFiles {
			if i == m.selectedFile {
				s += fmt.Sprintf("  > %s\n", file)
			} else {
				s += fmt.Sprintf("    %s\n", file)
			}
		}
		s += "\n  (Press Enter to select, Ctrl+D to quit)\n"
		return s
	}

	// TableView
	if m.isLoading {
		return "Loading parquet data..."
	}

	if m.error != nil {
		return fmt.Sprintf("Error: %v\n\nPress q or ESC to go back, Ctrl+D to quit", m.error)
	}

	if len(m.columns) == 0 {
		return "No data found in parquet file.\n\nPress q or ESC to go back, Ctrl+D to quit"
	}

	return "\n" + m.table.View() + "\n\nPress q or ESC to go back, Ctrl+D to quit"
}

// parquetDataMsg is a message containing parquet data
type parquetDataMsg struct {
	columns       []string
	rows          [][]string
	err           error
	totalRows     int
	parquetReader *reader.ParquetReader
	parquetFile   source.ParquetFile
}

// parquetFilesMsg is a message containing a list of parquet files
type parquetFilesMsg struct {
	files []string
}

// moreRowsMsg is a message containing additional rows loaded from the parquet file
type moreRowsMsg struct {
	newRow []string
	err    error
}

// findParquetFiles searches for parquet files in the current directory
func findParquetFiles() tea.Cmd {
	return func() tea.Msg {
		dir, err := os.Getwd()
		if err != nil {
			return parquetFilesMsg{files: []string{}}
		}

		files, err := os.ReadDir(dir)
		if err != nil {
			return parquetFilesMsg{files: []string{}}
		}

		var parquetFiles []string
		for _, file := range files {
			if !file.IsDir() && strings.HasSuffix(file.Name(), ".parquet") {
				parquetFiles = append(parquetFiles, file.Name())
			}
		}

		return parquetFilesMsg{files: parquetFiles}
	}
}

// loadParquetData loads data from a parquet file
func loadParquetData(filePath string, maxRows int) tea.Cmd {
	return func() tea.Msg {
		if filePath == "" {
			return parquetDataMsg{err: fmt.Errorf("no parquet file specified")}
		}

		// Open parquet file
		fr, err := local.NewLocalFileReader(filePath)
		if err != nil {
			return parquetDataMsg{err: fmt.Errorf("failed to open parquet file: %v", err)}
		}

		// Create parquet reader
		pr, err := reader.NewParquetReader(fr, nil, 4)
		if err != nil {
			err := fr.Close()
			if err != nil {
				return nil
			}
			return parquetDataMsg{err: fmt.Errorf("failed to create parquet reader: %v", err)}
		}

		// Read a sample row to determine columns
		sampleData, err := pr.ReadByNumber(1)
		if err != nil {
			pr.ReadStop()
			err := fr.Close()
			if err != nil {
				return nil
			}
			return parquetDataMsg{err: fmt.Errorf("failed to read sample data: %v", err)}
		}

		// Reset reader position
		pr.ReadStop()
		pr, err = reader.NewParquetReader(fr, nil, 4)
		if err != nil {
			err := fr.Close()
			if err != nil {
				return nil
			}
			return parquetDataMsg{err: fmt.Errorf("failed to reset parquet reader: %v", err)}
		}

		// Extract column names from sample data
		var columns []string
		if len(sampleData) > 0 {
			// Try to extract column names from the first row
			switch v := sampleData[0].(type) {
			case map[string]interface{}:
				// If it's a map, use the keys as column names
				for k := range v {
					columns = append(columns, k)
				}
			default:
				// If it's a struct or other type, use reflection to get field names
				val := reflect.ValueOf(v)
				if val.Kind() == reflect.Struct {
					typ := val.Type()
					for i := 0; i < typ.NumField(); i++ {
						field := typ.Field(i)
						// Check for parquet tag to get the actual column name
						tag := field.Tag.Get("parquet")
						if tag != "" {
							parts := strings.Split(tag, ",")
							for _, part := range parts {
								if strings.HasPrefix(part, "name=") {
									name := strings.TrimPrefix(part, "name=")
									name = strings.TrimSpace(name)
									columns = append(columns, name)
									break
								}
							}
						} else {
							// Use field name if no tag
							columns = append(columns, field.Name)
						}
					}
				}
			}
		}

		// If still no columns found, use a generic approach
		if len(columns) == 0 {
			pr.ReadStop()
			err := fr.Close()
			if err != nil {
				return nil
			}
			return parquetDataMsg{err: fmt.Errorf("could not determine columns from parquet file")}
		}

		// Get total number of rows in the file
		totalRows := int(pr.GetNumRows())

		// Read initial rows
		num := totalRows
		if num > maxRows {
			num = maxRows
		}

		rows := make([][]string, 0, num)
		for i := 0; i < num; i++ {
			rowData, err := pr.ReadByNumber(1)
			if err != nil {
				pr.ReadStop()
				err := fr.Close()
				if err != nil {
					return nil
				}
				return parquetDataMsg{err: fmt.Errorf("failed to read row %d: %v", i, err)}
			}

			if len(rowData) == 0 {
				continue
			}

			// Convert row data to strings
			row := make([]string, 0, len(columns))

			// Handle different data types
			switch data := rowData[0].(type) {
			case map[string]interface{}:
				// For map type data
				for _, col := range columns {
					if val, ok := data[col]; ok {
						row = append(row, fmt.Sprintf("%v", val))
					} else {
						row = append(row, "<nil>")
					}
				}
			default:
				// For struct type data using reflection
				val := reflect.ValueOf(data)
				if val.Kind() == reflect.Struct {
					for _, col := range columns {
						found := false
						for i := 0; i < val.NumField(); i++ {
							field := val.Type().Field(i)
							tag := field.Tag.Get("parquet")
							name := field.Name

							// Check if this field matches the column
							if tag != "" {
								parts := strings.Split(tag, ",")
								for _, part := range parts {
									if strings.HasPrefix(part, "name=") {
										tagName := strings.TrimPrefix(part, "name=")
										tagName = strings.TrimSpace(tagName)
										if tagName == col {
											row = append(row, fmt.Sprintf("%v", val.Field(i).Interface()))
											found = true
											break
										}
									}
								}
							} else if name == col {
								row = append(row, fmt.Sprintf("%v", val.Field(i).Interface()))
								found = true
								break
							}
						}
						if !found {
							row = append(row, "<nil>")
						}
					}
				} else {
					// For other types, just add placeholders
					for range columns {
						row = append(row, "<unsupported>")
					}
				}
			}

			rows = append(rows, row)
		}

		return parquetDataMsg{
			columns:       columns,
			rows:          rows,
			totalRows:     totalRows,
			parquetReader: pr,
			parquetFile:   fr,
		}
	}
}

// loadMoreRows loads one more row from the parquet file and updates the table
func loadMoreRows(m Model) tea.Cmd {
	return func() tea.Msg {
		if m.parquetReader == nil {
			return moreRowsMsg{err: fmt.Errorf("parquet reader is not initialized")}
		}

		// Read one more row
		rowData, err := m.parquetReader.ReadByNumber(1)
		if err != nil {
			return moreRowsMsg{err: fmt.Errorf("failed to read row: %v", err)}
		}

		if len(rowData) == 0 {
			return moreRowsMsg{err: fmt.Errorf("no more rows to read")}
		}

		// Convert row data to strings
		row := make([]string, 0, len(m.columns))

		// Handle different data types
		switch data := rowData[0].(type) {
		case map[string]interface{}:
			// For map type data
			for _, col := range m.columns {
				if val, ok := data[col]; ok {
					row = append(row, fmt.Sprintf("%v", val))
				} else {
					row = append(row, "<nil>")
				}
			}
		default:
			// For struct type data using reflection
			val := reflect.ValueOf(data)
			if val.Kind() == reflect.Struct {
				for _, col := range m.columns {
					found := false
					for i := 0; i < val.NumField(); i++ {
						field := val.Type().Field(i)
						tag := field.Tag.Get("parquet")
						name := field.Name

						// Check if this field matches the column
						if tag != "" {
							parts := strings.Split(tag, ",")
							for _, part := range parts {
								if strings.HasPrefix(part, "name=") {
									tagName := strings.TrimPrefix(part, "name=")
									tagName = strings.TrimSpace(tagName)
									if tagName == col {
										row = append(row, fmt.Sprintf("%v", val.Field(i).Interface()))
										found = true
										break
									}
								}
							}
						} else if name == col {
							row = append(row, fmt.Sprintf("%v", val.Field(i).Interface()))
							found = true
							break
						}
					}
					if !found {
						row = append(row, "<nil>")
					}
				}
			} else {
				// For other types, just add placeholders
				for range m.columns {
					row = append(row, "<unsupported>")
				}
			}
		}

		return moreRowsMsg{newRow: row}
	}
}

func main() {
	// Initialize text input
	ti := textinput.New()
	ti.Placeholder = "Path to parquet file"
	ti.Focus()
	ti.CharLimit = 256
	ti.Width = 80

	// Check if a file path was provided as command line argument
	filePath := ""
	viewState := InputView

	if len(os.Args) > 1 {
		filePath = os.Args[1]
		viewState = TableView
	}

	// Initialize model
	m := Model{
		filePath:         filePath,
		maxRows:          32,
		isLoading:        filePath != "",
		textInput:        ti,
		viewState:        viewState,
		parquetFiles:     []string{},
		selectedFile:     0,
		currentRowOffset: 0,
		totalRows:        0,
		parquetReader:    nil,
		parquetFile:      nil,
	}

	p := tea.NewProgram(m, tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		log.Fatal("Error running program:", err)
	}
}
