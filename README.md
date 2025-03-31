# Parquet File Viewer

A simple TUI application written in Go that visualizes the first few rows of a parquet file. This tool uses the bubbletea framework for creating an interactive terminal interface and parquet-go for parsing parquet files.

## Features

- Display the first 5 rows of any parquet file
- Interactive terminal UI with table view
- Column headers based on parquet schema
- Simple keyboard navigation

## Installation

### Prerequisites

- Go 1.20 or higher

### Building from source

```bash
# Clone the repository
git clone https://github.com/user/paquetChecker.git
cd paquetChecker

# Build the application. Please remove sample_generator before build
go build -o paquetChecker
```

## Usage

Place this application in the directory containing your parquet files and run it:

```bash
./paquetChecker
```

### Controls

- `q` or `Ctrl+D`: Quit the application

## Dependencies

- [bubbletea](https://github.com/charmbracelet/bubbletea): Terminal UI framework
- [bubbles](https://github.com/charmbracelet/bubbles): UI components for bubbletea
- [lipgloss](https://github.com/charmbracelet/lipgloss): Styling for terminal applications
- [parquet-go](https://github.com/xitongsys/parquet-go): Parquet file format implementation in Go

## License

GPL-3.0
