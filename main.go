package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/alecthomas/kong"
)

type Args struct {
	Input  string `short:"i" type:"existingfile" help:"Input file to parse" default:"-"`
	Output string `short:"o" type:"path" help:"File to write output" default:"-"`
}

type NameCount struct {
	Name  string `json:"name"`
	Count uint64 `json:"count"`
}

type Analysis struct {
	BlockID                                         string
	Duration                                        string
	Series                                          uint64
	LabelNames                                      uint64
	PostingsUnique                                  uint64
	PostingsEntries                                 uint64
	LabelPairsMostInvolvedInChurning                []NameCount
	LabelNamesMostInvolvedInChurning                []NameCount
	MostCommonLabelPairs                            []NameCount
	LabelNamesWithHighestCumulativeLabelValueLength []NameCount
	HighestCardinalityLabels                        []NameCount
	HighestCardinalityMetricNames                   []NameCount
}

func main() {
	var args Args
	_ = kong.Parse(&args)

	input, err := openInputFile(args.Input)
	if err != nil {
		panic(err)
	}
	defer input.Close()

	output, err := openOutputFile(args.Output)
	if err != nil {
		panic(err)
	}
	defer output.Close()

	analysis, err := parse(input)
	if err != nil {
		panic(err)
	}

	j, err := json.Marshal(analysis)
	if err != nil {
		panic(err)
	}
	_, err = output.Write(j)
}

func openInputFile(path string) (file *os.File, err error) {
	if path == "-" {
		return os.Stdin, nil
	}
	return os.Open(path)
}

func openOutputFile(path string) (file *os.File, err error) {
	if path == "-" {
		return os.Stdout, nil
	}
	return os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
}

func parse(input io.Reader) (analysis Analysis, err error) {
	scanner := bufio.NewScanner(input)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "Block ID: ") {
			analysis.BlockID = strings.TrimPrefix(line, "Block ID: ")
			continue
		}
		if strings.HasPrefix(line, "Duration: ") {
			analysis.Duration = strings.TrimPrefix(line, "Duration: ")
			continue
		}
		if strings.HasPrefix(line, "Series: ") {
			analysis.Series, err = strconv.ParseUint(strings.TrimPrefix(line, "Series: "), 10, 64)
			if err != nil {
				return analysis, err
			}
			continue
		}
		if strings.HasPrefix(line, "Label names: ") {
			analysis.LabelNames, err = strconv.ParseUint(strings.TrimPrefix(line, "Label names: "), 10, 64)
			if err != nil {
				return analysis, err
			}
			continue
		}
		if strings.HasPrefix(line, "Postings (unique label pairs): ") {
			analysis.PostingsUnique, err = strconv.ParseUint(strings.TrimPrefix(line, "Postings (unique label pairs): "), 10, 64)
			if err != nil {
				return analysis, err
			}
			continue
		}
		if strings.HasPrefix(line, "Postings entries (total label pairs): ") {
			analysis.PostingsEntries, err = strconv.ParseUint(strings.TrimPrefix(line, "Postings entries (total label pairs): "), 10, 64)
			if err != nil {
				return analysis, err
			}
			continue
		}

		if line == "Label pairs most involved in churning:" {
			values, err := extractValues(scanner)
			if err != nil {
				return analysis, err
			}
			analysis.LabelPairsMostInvolvedInChurning = values
			continue
		}

		if line == "Label names most involved in churning:" {
			values, err := extractValues(scanner)
			if err != nil {
				return analysis, err
			}
			analysis.LabelNamesMostInvolvedInChurning = values
			continue
		}

		if line == "Most common label pairs:" {
			values, err := extractValues(scanner)
			if err != nil {
				return analysis, err
			}
			analysis.MostCommonLabelPairs = values
			continue
		}

		if line == "Label names with highest cumulative label value length:" {
			values, err := extractValues(scanner)
			if err != nil {
				return analysis, err
			}
			analysis.LabelNamesWithHighestCumulativeLabelValueLength = values
			continue
		}

		if line == "Highest cardinality labels:" {
			values, err := extractValues(scanner)
			if err != nil {
				return analysis, err
			}
			analysis.HighestCardinalityLabels = values
			continue
		}

		if line == "Highest cardinality metric names:" {
			values, err := extractValues(scanner)
			if err != nil {
				return analysis, err
			}
			analysis.HighestCardinalityMetricNames = values
			continue
		}
	}
	return analysis, nil
}

func extractValues(scanner *bufio.Scanner) ([]NameCount, error) {
	var values []NameCount
	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			break
		}
		parts := strings.Split(line, " ")
		if len(parts) != 2 {
			return nil, fmt.Errorf("expected two parts in label pair %q", line)
		}
		count, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("expected uint64 as first part of label pair %q", line)
		}
		values = append(values, NameCount{parts[1], count})
	}
	return values, nil
}
