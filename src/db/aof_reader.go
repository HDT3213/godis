package db

import (
    "bufio"
    "github.com/HDT3213/godis/src/lib/logger"
    "io"
    "os"
    "strconv"
    "strings"
)

func (db *DB) loadAof() {
    file, err := os.Open(db.aofFilename)
    if err != nil {
        if _, ok := err.(*os.PathError); ok {
            return
        }
        logger.Warn(err)
        return
    }
    defer file.Close()

    reader := bufio.NewReader(file)
    var fixedLen int64 = 0
    var expectedArgsCount uint32
    var receivedCount uint32
    var args [][]byte
    processing := false
    var msg []byte
    for {
        if fixedLen == 0 {
            msg, err = reader.ReadBytes('\n')
            if err == io.EOF {
                return
            }
            if len(msg) == 0 || msg[len(msg)-2] != '\r' {
                logger.Warn("invalid format: line should end with \\r\\n")
                return
            }
        } else {
            msg = make([]byte, fixedLen+2)
            _, err = io.ReadFull(reader, msg)
            if err == io.EOF {
                return
            }
            if len(msg) == 0 ||
                msg[len(msg)-2] != '\r' ||
                msg[len(msg)-1] != '\n' {
                logger.Warn("invalid multibulk length")
                return
            }
            fixedLen = 0
        }
        if err != nil {
            logger.Warn(err)
            return
        }

        if !processing {
            // new request
            if msg[0] == '*' {
                // bulk multi msg
                expectedLine, err := strconv.ParseUint(string(msg[1:len(msg)-2]), 10, 32)
                if err != nil {
                    logger.Warn(err)
                    return
                }
                expectedArgsCount = uint32(expectedLine)
                receivedCount = 0
                processing = true
                args = make([][]byte, expectedLine)
            } else {
                logger.Warn("msg should start with '*'")
                return
            }
        } else {
            // receive following part of a request
            line := msg[0 : len(msg)-2]
            if line[0] == '$' {
                fixedLen, err = strconv.ParseInt(string(line[1:]), 10, 64)
                if err != nil {
                    logger.Warn(err)
                    return
                }
                if fixedLen <= 0 {
                    logger.Warn("invalid multibulk length")
                    return
                }
            } else {
                args[receivedCount] = line
                receivedCount++
            }

            // if sending finished
            if receivedCount == expectedArgsCount {
                processing = false

                cmd := strings.ToLower(string(args[0]))
                cmdFunc, ok := router[cmd]
                if ok {
                    cmdFunc(db, args[1:])
                }

                // finish
                expectedArgsCount = 0
                receivedCount = 0
                args = nil
            }
        }
    }
}
