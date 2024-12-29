package types

import (
	"container/list"
	"sync"
	"sync/atomic"

	"git.sr.ht/~rjarry/aerc/lib/log"
	"git.sr.ht/~rjarry/aerc/models"
)

type WorkerInteractor interface {
	log.Logger
	Actions() chan WorkerMessage
	ProcessAction(WorkerMessage) WorkerMessage
	PostAction(WorkerMessage, func(msg WorkerMessage))
	PostMessage(WorkerMessage)
	Unwrap() WorkerInteractor
	Name() string
}

type Backend interface {
	Run()
	Capabilities() *models.Capabilities
	PathSeparator() string
}

type Worker struct {
	Backend Backend

	actions         chan WorkerMessage
	actionCallbacks map[uint64]func(msg WorkerMessage)
	actionQueue     *list.List
	status          int32
	name            string

	sync.Mutex
	log.Logger
}

func NewWorker(name string) *Worker {
	return &Worker{
		Logger:          log.NewLogger(name, 2),
		actions:         make(chan WorkerMessage),
		actionCallbacks: make(map[uint64]func(msg WorkerMessage)),
		actionQueue:     list.New(),
		name:            name,
	}
}

func (worker *Worker) Unwrap() WorkerInteractor {
	return nil
}

func (worker *Worker) Actions() chan WorkerMessage {
	return worker.actions
}

func (worker *Worker) Name() string {
	return worker.name
}

var lastId atomic.Uint64

func (worker *Worker) setId(msg WorkerMessage) {
	msg.setId(lastId.Add(1))
}

const (
	idle int32 = iota
	busy
)

// Add a new task to the action queue without blocking. Start processing the
// queue in the background if needed.
func (worker *Worker) queue(msg WorkerMessage) {
	worker.Lock()
	defer worker.Unlock()
	worker.actionQueue.PushBack(msg)
	if atomic.LoadInt32(&worker.status) == idle {
		atomic.StoreInt32(&worker.status, busy)
		go worker.processQueue()
	}
}

// Start processing the action queue and write all messages to the actions
// channel, one by one. Stop when the action queue is empty.
func (worker *Worker) processQueue() {
	defer log.PanicHandler()
	for {
		worker.Lock()
		e := worker.actionQueue.Front()
		if e == nil {
			atomic.StoreInt32(&worker.status, idle)
			worker.Unlock()
			return
		}
		msg := worker.actionQueue.Remove(e).(WorkerMessage)
		worker.Unlock()
		worker.actions <- msg
	}
}

// PostAction posts an action to the worker. This method should not be called
// from the same goroutine that the worker runs in or deadlocks may occur
func (worker *Worker) PostAction(msg WorkerMessage, cb func(msg WorkerMessage)) {
	worker.setId(msg)
	if cb != nil {
		worker.Lock()
		worker.Infof("PostAction %T %d callback %+v", msg, msg.getId(), cb)
		worker.actionCallbacks[msg.getId()] = cb
		worker.Unlock()
	}
	// write to actions channel without blocking
	worker.queue(msg)
}

var WorkerMessages = make(chan WorkerMessage, 50)

// PostMessage posts an message to the UI. This method should not be called
// from the same goroutine that the UI runs in or deadlocks may occur
func (worker *Worker) PostMessage(msg WorkerMessage) {
	var reqid uint64
	req := msg.InResponseTo()
	if req != nil {
		reqid = req.getId()
	}
	worker.setId(msg)
	msg.setAccount(worker.name)
	worker.Infof("PostMessage %T %d (reply to %d)", msg, msg.getId(), reqid)
	WorkerMessages <- msg
	worker.Infof("PostMessage %T %d (reply to %d) ok", msg, msg.getId(), reqid)
}

func (worker *Worker) ProcessMessage(msg WorkerMessage) WorkerMessage {
	worker.Infof("ProcessMessage %T %d", msg, msg.getId())
	if inResponseTo := msg.InResponseTo(); inResponseTo != nil {
		worker.Lock()
		f, ok := worker.actionCallbacks[inResponseTo.getId()]
		worker.Unlock()
		if ok {
			worker.Infof("ProcessMessage %T %d callback %+v", msg, msg.getId(), f)
			f(msg)
			switch msg.(type) {
			case *Cancelled, *Done:
				worker.Lock()
				delete(worker.actionCallbacks, inResponseTo.getId())
				worker.Unlock()
			}
		}
	}
	if msg.Cancelled() {
		worker.Infof("ProcessMessage %T %d cancelled", msg, msg.getId())
		msg = &Cancelled{Message: RespondTo(msg.InResponseTo())}
	}
	return msg
}

func (worker *Worker) ProcessAction(msg WorkerMessage) WorkerMessage {
	worker.Infof("ProcessAction %T %d", msg, msg.getId())
	if msg.Cancelled() {
		worker.PostMessage(&Cancelled{Message: RespondTo(msg)})
	} else if _, ok := msg.(*Noop); ok {
		worker.PostMessage(&Done{Message: RespondTo(msg)})
	}
	return msg
}

func (worker *Worker) PathSeparator() string {
	return worker.Backend.PathSeparator()
}
