package zdmproxy

import (
	"bytes"
	"context"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	appd "github.com/datastax/zdm-proxy/appdynamics"
	"github.com/datastax/zdm-proxy/proxy/pkg/config"
	log "github.com/sirupsen/logrus"
	"net"
	"strconv"
	"strings"
	"sync"
)

const (
	initialBufferSize = 1024
)

// Coalesces writes using a write buffer
type writeCoalescer struct {
	connection net.Conn
	conf       *config.Config

	clientHandlerWaitGroup *sync.WaitGroup
	shutdownContext        context.Context
	cancelFunc             context.CancelFunc

	writeQueue chan *frame.RawFrame

	logPrefix string

	waitGroup *sync.WaitGroup

	writeBufferSizeBytes int

	scheduler *Scheduler

	streamToBtHandle     map[string]appd.BtHandle
	streamToBtHandleLock *sync.Mutex
}

func NewWriteCoalescer(
	conf *config.Config,
	conn net.Conn,
	clientHandlerWaitGroup *sync.WaitGroup,
	shutdownContext context.Context,
	clientHandlerCancelFunc context.CancelFunc,
	logPrefix string,
	isRequest bool,
	isAsync bool,
	scheduler *Scheduler) *writeCoalescer {

	writeQueueSizeFrames := conf.RequestWriteQueueSizeFrames
	if !isRequest {
		writeQueueSizeFrames = conf.ResponseWriteQueueSizeFrames
	}
	if isAsync {
		writeQueueSizeFrames = conf.AsyncConnectorWriteQueueSizeFrames
	}

	writeBufferSizeBytes := conf.RequestWriteBufferSizeBytes
	if !isRequest {
		writeBufferSizeBytes = conf.ResponseWriteBufferSizeBytes
	}
	if isAsync {
		writeBufferSizeBytes = conf.AsyncConnectorWriteBufferSizeBytes
	}
	return &writeCoalescer{
		connection:             conn,
		conf:                   conf,
		clientHandlerWaitGroup: clientHandlerWaitGroup,
		shutdownContext:        shutdownContext,
		cancelFunc:             clientHandlerCancelFunc,
		writeQueue:             make(chan *frame.RawFrame, writeQueueSizeFrames),
		logPrefix:              logPrefix,
		waitGroup:              &sync.WaitGroup{},
		writeBufferSizeBytes:   writeBufferSizeBytes,
		scheduler:              scheduler,
		streamToBtHandle:       make(map[string]appd.BtHandle),
		streamToBtHandleLock:   &sync.Mutex{},
	}
}

func (recv *writeCoalescer) RunWriteQueueLoop() {
	connectionAddr := recv.connection.RemoteAddr().String()
	log.Tracef("[%v] WriteQueueLoop starting for %v", recv.logPrefix, connectionAddr)

	recv.clientHandlerWaitGroup.Add(1)
	recv.waitGroup.Add(1)
	go func() {
		defer recv.clientHandlerWaitGroup.Done()
		defer recv.waitGroup.Done()

		draining := false
		bufferedWriter := bytes.NewBuffer(make([]byte, 0, initialBufferSize))
		wg := &sync.WaitGroup{}
		defer wg.Wait()

		for {
			var resultOk bool
			var result *coalescerIterationResult

			firstFrame, firstFrameOk := <-recv.writeQueue
			if !firstFrameOk {
				break
			}

			resultChannel := make(chan *coalescerIterationResult, 1)
			tempDraining := draining
			tempBuffer := bufferedWriter
			wg.Add(1)
			recv.scheduler.Schedule(func() {
				defer wg.Done()
				firstFrameRead := false
				for {
					var f *frame.RawFrame
					var ok bool
					if firstFrameRead {
						select {
						case f, ok = <-recv.writeQueue:
						default:
							ok = false
						}

						if !ok {
							t := &coalescerIterationResult{
								buffer:   tempBuffer,
								draining: tempDraining,
							}
							resultChannel <- t
							close(resultChannel)
							return
						}

						if tempDraining {
							// continue draining the write queue without writing on connection until it is closed
							log.Tracef("[%v] Discarding frame from write queue because shutdown was requested: %v", recv.logPrefix, f.Header)
							continue
						}
					} else {
						firstFrameRead = true
						f = firstFrame
						ok = true
					}

					log.Tracef("[%v] Writing %v on %v", recv.logPrefix, f.Header, connectionAddr)
					err := writeRawFrame(tempBuffer, connectionAddr, recv.shutdownContext, f)
					recv.EndFrameBT(f)
					if err != nil {
						tempDraining = true
						handleConnectionError(err, recv.shutdownContext, recv.cancelFunc, recv.logPrefix, "writing", connectionAddr)
					} else {
						if tempBuffer.Len() >= recv.writeBufferSizeBytes {
							t := &coalescerIterationResult{
								buffer:   tempBuffer,
								draining: tempDraining,
							}
							resultChannel <- t
							close(resultChannel)
							return
						}
					}
				}
			})

			result, resultOk = <-resultChannel
			if !resultOk {
				break
			}

			draining = result.draining
			bufferedWriter = result.buffer
			if bufferedWriter.Len() > 0 && !draining {
				_, err := recv.connection.Write(bufferedWriter.Bytes())
				bufferedWriter.Reset()
				if err != nil {
					handleConnectionError(err, recv.shutdownContext, recv.cancelFunc, recv.logPrefix, "writing", connectionAddr)
					draining = true
				}
			}
		}
	}()
}

func (recv *writeCoalescer) Enqueue(frame *frame.RawFrame) {
	log.Tracef("[%v] Sending %v to write queue on %v", recv.logPrefix, frame.Header, recv.connection.RemoteAddr())
	recv.writeQueue <- frame
	log.Tracef("[%v] Sent %v to write queue on %v", recv.logPrefix, frame.Header, recv.connection.RemoteAddr())
}

func (recv *writeCoalescer) EnqueueAsync(frame *frame.RawFrame) bool {
	log.Tracef("[%v] Sending %v to write queue on %v", recv.logPrefix, frame.Header, recv.connection.RemoteAddr())
	select {
	case recv.writeQueue <- frame:
		log.Tracef("[%v] Sent %v to write queue on %v", recv.logPrefix, frame.Header, recv.connection.RemoteAddr())
		return true
	default:
		log.Debugf("[%v] Discarded %v because write queue is full on %v", recv.logPrefix, frame.Header, recv.connection.RemoteAddr())
		return false
	}
}

func (recv *writeCoalescer) Close() {
	close(recv.writeQueue)
	recv.waitGroup.Wait()
}

func (recv *writeCoalescer) getStatementInfo(frame *frame.RawFrame) (string, string, *statementQueryData, error) {
	var requestType string
	currentKeyspace := ""
	frameContext := NewFrameDecodeContext(frame)
	var timeUuidGenerator TimeUuidGenerator

	decodedFrame, err := frameContext.GetOrDecodeFrame()
	if err != nil {
		return "", "", nil, fmt.Errorf("failed to decode frame: %w", err)
	}

	switch typedMsg := decodedFrame.Body.Message.(type) {
	case *message.Query:
		requestType = "query"
		currentKeyspace = strings.ToLower(typedMsg.Options.Keyspace)
	case *message.Prepare:
		requestType = "prepare"
		currentKeyspace = strings.ToLower(typedMsg.Keyspace)
	case *message.Batch:
		requestType = "batch"
		currentKeyspace = strings.ToLower(typedMsg.Keyspace)
	default:
		requestType = "unknown"
		currentKeyspace = ""
	}

	stmtQueryData, err := frameContext.GetOrInspectStatement(currentKeyspace, timeUuidGenerator)
	if err != nil {
		return "", "", nil, fmt.Errorf("failed to inspect %s frame: %w", requestType, err)
	}

	if currentKeyspace == "" {
		currentKeyspace = stmtQueryData.queryData.getApplicableKeyspace()
	}

	return requestType, currentKeyspace, stmtQueryData, nil
}

func getStreamBtHandleId(frame *frame.RawFrame) string {
	return strconv.Itoa(int(frame.Header.StreamId))
}

func (recv *writeCoalescer) StartFrameBTFromQueryData(frame *frame.RawFrame, name string, stmtQueryData *statementQueryData) {
	if recv.conf.AppdEnabled {
		streamBtHandleId := getStreamBtHandleId(frame)

		if frame.Header.OpCode != primitive.OpCodeBatch {
			if frame.Header.OpCode == primitive.OpCodeQuery || frame.Header.OpCode == primitive.OpCodePrepare {
				if stmtQueryData == nil {
					log.Warnf("[%v] Ignoring AppDynamics Business Transaction for stream id: %s as the statement query data is nil.", recv.logPrefix, streamBtHandleId)
					return
				}
				// If the query has no keyspace or table, we ignore it. This catches frames containing a query opcode
				// and a metadata payload. For example, DSE insights diagnostic data that is generated by the DSE
				// driver which contain a query opcode but no Data Manipulation (SELECT, INSERT, etc.) query.
				if stmtQueryData.queryData.getTableName() == "" || stmtQueryData.queryData.getApplicableKeyspace() == "" {
					log.Tracef("[%v] Ignoring AppDynamics Business Transaction for stream id: %s as the table name or keyspace is undefined in the query.", recv.logPrefix, streamBtHandleId)
					return
				}

				// If the query is a system query, we ignore it. Only check query or prepare requests as batch request
				// should never contain system queries.
				if recv.conf.AppdIgnoreSystemKeyspaceQueries && isSystemQuery(stmtQueryData.queryData) {
					log.Tracef("[%v] Ignoring AppDynamics Business Transaction for stream id: %s as it contains a system keyspace query.", recv.logPrefix, streamBtHandleId)
					return
				}
			} else {
				log.Tracef("[%v] Ignoring AppDynamics Business Transaction for stream id: %s as it is not a query, prepare, or batch operation.", recv.logPrefix, streamBtHandleId)
				return
			}
		}

		recv.streamToBtHandleLock.Lock()
		defer recv.streamToBtHandleLock.Unlock()
		log.Debugf("[%v] Starting AppDynamics Business Transaction for stream id: %s; opType: %s", recv.logPrefix, streamBtHandleId, frame.Header.OpCode.String())
		btHandle := appd.StartBT(name, "")
		appd.AddUserDataToBT(btHandle, "OperationType", frame.Header.OpCode.String())
		recv.streamToBtHandle[streamBtHandleId] = btHandle
	}
}

func (recv *writeCoalescer) EndFrameBT(frame *frame.RawFrame) {
	if recv.conf.AppdEnabled {
		streamBtHandleId := getStreamBtHandleId(frame)
		recv.streamToBtHandleLock.Lock()
		defer recv.streamToBtHandleLock.Unlock()
		btHandle, ok := recv.streamToBtHandle[streamBtHandleId]
		if ok {
			log.Debugf("[%s] Ending AppDynamics Business Transaction for stream id: %s", recv.logPrefix, streamBtHandleId)
			appd.EndBT(btHandle)
			delete(recv.streamToBtHandle, streamBtHandleId)
		}
	}
}

type coalescerIterationResult struct {
	buffer   *bytes.Buffer
	draining bool
}
