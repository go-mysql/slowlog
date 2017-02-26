/*
	Copyright 2017 Daniel Nichter
	Copyright 2014-2016 Percona LLC and/or its affiliates
*/

// Package slowlog provides functions and data structures for working with the
// MySQL slow log.
package slowlog

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
)

var (
	// ErrStarted is returned if Parser.Start is called more than once.
	ErrStarted = errors.New("parser is started")
)

// Options encapsulate common options for making a new LogParser.
type Options struct {
	StartOffset        uint64          // byte offset in file at which to start parsing
	FilterAdminCommand map[string]bool // admin commands to ignore
}

// A Parser parses events from a slow log. The canonical Parser is FileParser
// because the slow log is a file. The caller receives events on the Events
// channel. This channel is closed when there are no more events. Any error
// during parsing is returned by Error.
type Parser interface {
	Start(Options) error
	Events() <-chan Event
	Stop()
	Error() error
}

// Regular expressions to match important lines in slow log.
var timeRe = regexp.MustCompile(`Time: (\S+\s{1,2}\S+)`)
var userRe = regexp.MustCompile(`User@Host: ([^\[]+|\[[^[]+\]).*?@ (\S*) \[(.*)\]`)
var schema = regexp.MustCompile(`Schema: +(.*?) +Last_errno:`)
var headerRe = regexp.MustCompile(`^#\s+[A-Z]`)
var metricsRe = regexp.MustCompile(`(\w+): (\S+|\z)`)
var adminRe = regexp.MustCompile(`command: (.+)`)
var setRe = regexp.MustCompile(`^SET (?:last_insert_id|insert_id|timestamp)`)
var useRe = regexp.MustCompile(`^(?i)use `)

// FileParser represents a file-based Parser. This is the canonical Parser
// because the slow log is a file.
type FileParser struct {
	file *os.File
	// --
	opt         Options
	stopChan    chan struct{}
	eventChan   chan Event
	inHeader    bool
	inQuery     bool
	headerLines uint
	queryLines  uint64
	bytesRead   uint64
	lineOffset  uint64
	started     bool
	event       *Event
	err         error
}

var Debug = false

// NewFileParser returns a new FileParser that reads from the open file.
// The file is not closed.
func NewFileParser(file *os.File) *FileParser {
	p := &FileParser{
		file: file,
		// --
		stopChan:    make(chan struct{}),
		eventChan:   make(chan Event),
		inHeader:    false,
		inQuery:     false,
		headerLines: 0,
		queryLines:  0,
		lineOffset:  0,
		event:       NewEvent(),
	}
	return p
}

// Stop stops the parser before parsing the next event or while blocked on
// sending the current event to the event channel.
func (p *FileParser) Stop() {
	if Debug {
		log.Println("stopping")
	}
	if !p.started {
		return
	}
	close(p.stopChan)
	return
}

// Start starts the parser. Events are sent to the unbuffered Events channel.
// Parsing stops on EOF, error, or call to Stop. The Events channel is closed
// when parsing stops.
func (p *FileParser) Start(opt Options) error {
	if p.started {
		return ErrStarted
	}

	p.opt = opt

	// Seek to the offset, if any.
	if p.opt.StartOffset > 0 {
		if _, err := p.file.Seek(int64(p.opt.StartOffset), os.SEEK_SET); err != nil {
			return err
		}
	}

	p.bytesRead = opt.StartOffset

	go p.parse()
	p.started = true

	return nil
}

// Events returns the channel to which events from the slow log are sent.
// The channel is closed when there are no more events. Events are not sent
// until Start is called.
func (p *FileParser) Events() <-chan Event {
	return p.eventChan
}

// Error returns an error, if any, encountered while parsing the slow log.
func (p *FileParser) Error() error {
	return p.err
}

// --------------------------------------------------------------------------

func (p *FileParser) parse() {
	defer func() {
		if e := recover(); e != nil {
			p.err = fmt.Errorf("crash: %s", e)
		}
	}()

	defer close(p.eventChan)

	if Debug {
		log.SetFlags(log.Ltime | log.Lmicroseconds)
		fmt.Println()
		log.Println("parsing " + p.file.Name())
	}

	r := bufio.NewReader(p.file)

SCANNER_LOOP:
	for {
		select {
		case <-p.stopChan:
			return
		default:
		}

		line, err := r.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				p.err = fmt.Errorf("bufio.NewReader.ReadString: %s", err)
				return
			}
			break SCANNER_LOOP
		}

		lineLen := uint64(len(line))
		p.bytesRead += lineLen
		p.lineOffset = p.bytesRead - lineLen
		if p.lineOffset != 0 {
			// @todo Need to get clear on why this is needed;
			// it does make the value correct; an off-by-one issue
			p.lineOffset += 1
		}

		if Debug {
			fmt.Println()
			log.Printf("+%d line: %s", p.lineOffset, line)
		}

		// Filter out meta lines:
		//   /usr/local/bin/mysqld, Version: 5.6.15-62.0-tokudb-7.1.0-tokudb-log (binary). started with:
		//   Tcp port: 3306  Unix socket: /var/lib/mysql/mysql.sock
		//   Time                 Id Command    Argument
		if lineLen >= 20 && ((line[0] == '/' && line[lineLen-6:lineLen] == "with:\n") ||
			(line[0:5] == "Time ") ||
			(line[0:4] == "Tcp ") ||
			(line[0:4] == "TCP ")) {
			if Debug {
				log.Println("meta")
			}
			continue
		}

		// Remove \n.
		line = line[0 : lineLen-1]

		if p.inHeader {
			p.parseHeader(line)
		} else if p.inQuery {
			p.parseQuery(line)
		} else if headerRe.MatchString(line) {
			p.inHeader = true
			p.inQuery = false
			p.parseHeader(line)
		}
	}

	if p.queryLines > 0 {
		p.sendEvent(false, false)
	}

	if Debug {
		log.Printf("\ndone")
	}
}

// --------------------------------------------------------------------------

func (p *FileParser) parseHeader(line string) {
	if Debug {
		log.Println("header")
	}

	if !headerRe.MatchString(line) {
		p.inHeader = false
		p.inQuery = true
		p.parseQuery(line)
		return
	}

	if p.headerLines == 0 {
		p.event.Offset = p.lineOffset
	}
	p.headerLines++

	if strings.HasPrefix(line, "# Time") {
		if Debug {
			log.Println("time")
		}
		m := timeRe.FindStringSubmatch(line)
		if len(m) < 2 {
			return
		}
		p.event.Ts = m[1]
		if userRe.MatchString(line) {
			if Debug {
				log.Println("user (bad format)")
			}
			m := userRe.FindStringSubmatch(line)
			p.event.User = m[1]
			p.event.Host = m[2]
		}
	} else if strings.HasPrefix(line, "# User") {
		if Debug {
			log.Println("user")
		}
		m := userRe.FindStringSubmatch(line)
		if len(m) < 3 {
			return
		}
		p.event.User = m[1]
		p.event.Host = m[2]
	} else if strings.HasPrefix(line, "# admin") {
		p.parseAdmin(line)
	} else {
		if Debug {
			log.Println("metrics")
		}
		submatch := schema.FindStringSubmatch(line)
		if len(submatch) == 2 {
			p.event.Db = submatch[1]
		}

		m := metricsRe.FindAllStringSubmatch(line, -1)
		for _, smv := range m {
			// [String, Metric, Value], e.g. ["Query_time: 2", "Query_time", "2"]
			if strings.HasSuffix(smv[1], "_time") || strings.HasSuffix(smv[1], "_wait") {
				// microsecond value
				val, _ := strconv.ParseFloat(smv[2], 32)
				p.event.TimeMetrics[smv[1]] = float64(val)
			} else if smv[2] == "Yes" || smv[2] == "No" {
				// boolean value
				if smv[2] == "Yes" {
					p.event.BoolMetrics[smv[1]] = true
				} else {
					p.event.BoolMetrics[smv[1]] = false
				}
			} else if smv[1] == "Schema" {
				p.event.Db = smv[2]
			} else if smv[1] == "Log_slow_rate_type" {
				p.event.RateType = smv[2]
			} else if smv[1] == "Log_slow_rate_limit" {
				val, _ := strconv.ParseUint(smv[2], 10, 64)
				p.event.RateLimit = uint(val)
			} else if smv[1] == "InnoDB_trx_id" {
				continue // ignore
			} else {
				// integer value
				val, _ := strconv.ParseUint(smv[2], 10, 64)
				p.event.NumberMetrics[smv[1]] = val
			}
		}
	}
}

func (p *FileParser) parseQuery(line string) {
	if Debug {
		log.Println("query")
	}

	if strings.HasPrefix(line, "# admin") {
		p.parseAdmin(line)
		return
	} else if headerRe.MatchString(line) {
		if Debug {
			log.Println("next event")
		}
		p.inHeader = true
		p.inQuery = false
		p.sendEvent(true, false)
		p.parseHeader(line)
		return
	}

	isUse := useRe.FindString(line)
	if p.queryLines == 0 && isUse != "" {
		if Debug {
			log.Println("use db")
		}
		db := strings.TrimPrefix(line, isUse)
		db = strings.TrimRight(db, ";")
		db = strings.Trim(db, "`")
		p.event.Db = db
		// Set the 'use' as the query itself.
		// In case we are on a group of lines like in test 23, lines 6~8, the
		// query will be replaced by the real query "select field...."
		// In case we are on a group of lines like in test23, lines 27~28, the
		// query will be "use dbnameb" since the user executed a use command
		p.event.Query = line
	} else if setRe.MatchString(line) {
		if Debug {
			log.Println("set var")
		}
		// @todo ignore or use these lines?
	} else {
		if Debug {
			log.Println("query")
		}
		if p.queryLines > 0 {
			p.event.Query += "\n" + line
		} else {
			p.event.Query = line
		}
		p.queryLines++
	}
}

func (p *FileParser) parseAdmin(line string) {
	if Debug {
		log.Println("admin")
	}
	p.event.Admin = true
	m := adminRe.FindStringSubmatch(line)
	p.event.Query = m[1]
	p.event.Query = strings.TrimSuffix(p.event.Query, ";") // makes FilterAdminCommand work

	// admin commands should be the last line of the event.
	if filtered := p.opt.FilterAdminCommand[p.event.Query]; !filtered {
		if Debug {
			log.Println("not filtered")
		}
		p.sendEvent(false, false)
	} else {
		p.inHeader = false
		p.inQuery = false
	}
}

func (p *FileParser) sendEvent(inHeader bool, inQuery bool) {
	if Debug {
		log.Println("send event")
	}

	// Make a new event and reset our metadata.
	defer func() {
		p.event = NewEvent()
		p.headerLines = 0
		p.queryLines = 0
		p.inHeader = inHeader
		p.inQuery = inQuery
	}()

	if _, ok := p.event.TimeMetrics["Query_time"]; !ok {
		if p.headerLines == 0 {
			log.Panicf("no Query_time in event at %d: %#v", p.lineOffset, p.event)
		}
		// Started parsing in header after Query_time.  Throw away event.
		return
	}

	// Clean up the event.
	p.event.Db = strings.TrimSuffix(p.event.Db, ";\n")
	p.event.Query = strings.TrimSuffix(p.event.Query, ";")

	// Send the event.  This will block.
	select {
	case p.eventChan <- *p.event:
	case <-p.stopChan:
	}
}
