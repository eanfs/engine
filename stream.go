package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	. "github.com/logrusorgru/aurora/v4"
	"go.uber.org/zap"
	"m7s.live/engine/v4/common"
	"m7s.live/engine/v4/config"
	"m7s.live/engine/v4/log"
	"m7s.live/engine/v4/track"
	"m7s.live/engine/v4/util"
)

type StreamState byte
type StreamAction byte

func (s StreamState) String() string {
	return StateNames[s]
}
func (s StreamAction) String() string {
	return ActionNames[s]
}

// 四状态机
const (
	STATE_WAITPUBLISH StreamState = iota // 等待发布者状态
	STATE_WAITTRACK                      // 等待音视频轨道激活
	STATE_PUBLISHING                     // 正在发布流状态
	STATE_WAITCLOSE                      // 等待关闭状态(自动关闭延时开启)
	STATE_CLOSED                         // 流已关闭，不可使用
)

const (
	ACTION_PUBLISH        StreamAction = iota
	ACTION_TRACKAVAILABLE              // 音视频轨道激活
	ACTION_TIMEOUT                     // 发布流长时间没有数据/长时间没有发布者发布流/等待关闭时间到
	ACTION_PUBLISHCLOSE                // 发布者关闭
	ACTION_CLOSE                       // 主动关闭流
	ACTION_LASTLEAVE                   // 最后一个订阅者离开
	ACTION_FIRSTENTER                  // 第一个订阅者进入
	ACTION_NOTRACK                     // 没有音视频轨道
)

var StateNames = [...]string{"⌛", "🟡", "🟢", "🟠", "🔴"}
var ActionNames = [...]string{"publish", "track available", "timeout", "publish close", "close", "last leave", "first enter", "no tracks"}

/*
stateDiagram-v2
    [*] --> ⌛等待发布者 : 创建
    ⌛等待发布者 --> 🟡等待轨道 :发布
    ⌛等待发布者 --> 🔴已关闭 :关闭
    ⌛等待发布者 --> 🔴已关闭  :超时
    ⌛等待发布者 --> 🔴已关闭  :最后订阅者离开
		🟡等待轨道 --> 🟢正在发布 :轨道激活
		🟡等待轨道 --> 🔴已关闭 :关闭
		🟡等待轨道 --> 🔴已关闭 :超时
		🟡等待轨道 --> 🔴已关闭 :最后订阅者离开
    🟢正在发布 --> ⌛等待发布者: 发布者断开
    🟢正在发布 --> 🟠等待关闭: 最后订阅者离开
    🟢正在发布 --> 🔴已关闭  :关闭
    🟠等待关闭 --> 🟢正在发布 :第一个订阅者进入
    🟠等待关闭 --> 🔴已关闭  :关闭
    🟠等待关闭 --> 🔴已关闭  :超时
    🟠等待关闭 --> 🔴已关闭  :发布者断开
*/

var StreamFSM = [len(StateNames)]map[StreamAction]StreamState{
	{
		ACTION_PUBLISH:   STATE_WAITTRACK,
		ACTION_TIMEOUT:   STATE_CLOSED,
		ACTION_LASTLEAVE: STATE_CLOSED,
		ACTION_CLOSE:     STATE_CLOSED,
	},
	{
		ACTION_TRACKAVAILABLE: STATE_PUBLISHING,
		ACTION_TIMEOUT:        STATE_CLOSED,
		ACTION_LASTLEAVE:      STATE_WAITCLOSE,
		ACTION_CLOSE:          STATE_CLOSED,
	},
	{
		// ACTION_PUBLISHCLOSE: STATE_WAITPUBLISH,
		ACTION_TIMEOUT:   STATE_WAITPUBLISH,
		ACTION_LASTLEAVE: STATE_WAITCLOSE,
		ACTION_CLOSE:     STATE_CLOSED,
	},
	{
		// ACTION_PUBLISHCLOSE: STATE_CLOSED,
		ACTION_TIMEOUT:    STATE_CLOSED,
		ACTION_FIRSTENTER: STATE_PUBLISHING,
		ACTION_CLOSE:      STATE_CLOSED,
	},
	{},
}

// Streams 所有的流集合
var Streams util.Map[string, *Stream]

func FilterStreams[T IPublisher]() (ss []*Stream) {
	Streams.Range(func(_ string, s *Stream) {
		if _, ok := s.Publisher.(T); ok {
			ss = append(ss, s)
		}
	})
	return
}

type StreamTimeoutConfig struct {
	PublishTimeout    time.Duration //发布者无数据后超时
	DelayCloseTimeout time.Duration //无订阅者后超时,必须先有一次订阅才会激活
	IdleTimeout       time.Duration //无订阅者后超时，不需要订阅即可激活
	PauseTimeout      time.Duration //暂停后超时
	NeverTimeout      bool          // 永不超时
}
type Tracks struct {
	sync.Map
	Video       []*track.Video
	Audio       []*track.Audio
	Data        []common.Track
	MainVideo   *track.Video
	MainAudio   *track.Audio
	marshalLock sync.Mutex
}

func (tracks *Tracks) Range(f func(name string, t common.Track)) {
	tracks.Map.Range(func(k, v any) bool {
		f(k.(string), v.(common.Track))
		return true
	})
}

func (tracks *Tracks) Add(name string, t common.Track) bool {
	switch v := t.(type) {
	case *track.Video:
		if tracks.MainVideo == nil {
			tracks.MainVideo = v
			tracks.SetIDR(v)
		}
	case *track.Audio:
		if tracks.MainAudio == nil {
			tracks.MainAudio = v
		}
		if tracks.MainVideo != nil {
			v.Narrow()
		}
	}
	_, loaded := tracks.LoadOrStore(name, t)
	if !loaded {
		switch v := t.(type) {
		case *track.Video:
			tracks.Video = append(tracks.Video, v)
		case *track.Audio:
			tracks.Audio = append(tracks.Audio, v)
		default:
			tracks.Data = append(tracks.Data, v)
		}
	}
	return !loaded
}

func (tracks *Tracks) SetIDR(video common.Track) {
	if video == tracks.MainVideo {
		tracks.Range(func(_ string, t common.Track) {
			if v, ok := t.(*track.Audio); ok {
				v.Narrow()
			}
		})
	}
}

func (tracks *Tracks) MarshalJSON() ([]byte, error) {
	var trackList []common.Track
	tracks.marshalLock.Lock()
	defer tracks.marshalLock.Unlock()
	tracks.Range(func(_ string, t common.Track) {
		t.SnapForJson()
		trackList = append(trackList, t)
	})
	return json.Marshal(trackList)
}

var streamIdGen atomic.Uint32

// Stream 流定义
type Stream struct {
	timeout    *time.Timer //当前状态的超时定时器
	actionChan util.SafeChan[any]
	ID         uint32 // 流ID
	*log.Logger
	StartTime time.Time //创建时间
	StreamTimeoutConfig
	Path        string
	Publisher   IPublisher
	publisher   *Publisher
	State       StreamState
	SEHistory   []StateEvent // 事件历史
	Subscribers Subscribers  // 订阅者
	Tracks      Tracks
	AppName     string
	StreamName  string
	IsPause     bool // 是否处于暂停状态
	pubLocker   sync.Mutex
}
type StreamSummay struct {
	Path        string
	State       StreamState
	Subscribers int
	Tracks      []string
	StartTime   time.Time
	Type        string
	BPS         int
}

func (s *Stream) GetType() string {
	if s.Publisher == nil {
		return ""
	}
	return s.publisher.Type
}

func (s *Stream) GetPath() string {
	return s.Path
}

func (s *Stream) GetStartTime() time.Time {
	return s.StartTime
}

func (s *Stream) GetPublisherConfig() *config.Publish {
	if s.Publisher == nil {
		s.Error("GetPublisherConfig: Publisher is nil")
		return nil
	}
	return s.Publisher.GetConfig()
}

// Summary 返回流的简要信息
func (s *Stream) Summary() (r StreamSummay) {
	if s.publisher != nil {
		r.Type = s.publisher.Type
	}
	s.Tracks.Range(func(name string, t common.Track) {
		r.BPS += t.GetBPS()
		r.Tracks = append(r.Tracks, name)
	})
	r.Path = s.Path
	r.State = s.State
	r.Subscribers = s.Subscribers.Len()
	r.StartTime = s.StartTime
	return
}

func (s *Stream) SSRC() uint32 {
	return uint32(uintptr(unsafe.Pointer(s)))
}
func (s *Stream) SetIDR(video common.Track) {
	s.Tracks.SetIDR(video)
}
func findOrCreateStream(streamPath string, waitTimeout time.Duration) (s *Stream, created bool) {
	p := strings.Split(streamPath, "/")
	pl := len(p)
	if pl < 2 {
		log.Warn(Red("Stream Path Format Error:"), streamPath)
		return nil, false
	}
	actual, loaded := Streams.LoadOrStore(streamPath, &Stream{
		Path:       streamPath,
		AppName:    strings.Join(p[1:pl-1], "/"),
		StreamName: p[pl-1],
		StartTime:  time.Now(),
		timeout:    time.NewTimer(waitTimeout),
	})
	if s := actual.(*Stream); loaded {
		for s.Logger == nil {
			runtime.Gosched()
		}
		s.Debug("found")
		return s, false
	} else {
		s.ID = streamIdGen.Add(1)
		s.Subscribers.Init()
		s.actionChan.Init(10)
		s.Logger = log.LocaleLogger.With(zap.String("stream", streamPath), zap.Uint32("id", s.ID))
		s.Info("created")
		go s.run()
		return s, true
	}
}

func (r *Stream) resetTimer(dur time.Duration) {
		r.Debug("reset timer", zap.Duration("timeout", dur))
		r.timeout.Reset(dur)
}

func (r *Stream) action(action StreamAction) (ok bool) {
	var event StateEvent
	event.Target = r
	event.Action = action
	event.From = r.State
	event.Time = time.Now()
	var next StreamState
	if next, ok = event.Next(); ok {
		r.State = next
		r.SEHistory = append(r.SEHistory, event)
		// 给Publisher状态变更的回调，方便进行远程拉流等操作
		var stateEvent any
		r.Info(Sprintf("%s%s%s", event.From.String(), Yellow("->"), next.String()), zap.String("action", action.String()))
		switch next {
		case STATE_WAITPUBLISH:
			stateEvent = SEwaitPublish{event, r.Publisher}
			waitTime := time.Duration(0)
			if r.Publisher != nil {
				waitTime = r.Publisher.GetConfig().WaitCloseTimeout
				r.Tracks.Range(func(name string, t common.Track) {
					t.SetStuff(common.TrackStateOffline)
				})
			}
			r.Subscribers.OnPublisherLost(event)
			if suber := r.Subscribers.Pick(); suber != nil {
				r.Subscribers.Broadcast(stateEvent)
				if waitTime == 0 {
					waitTime = suber.GetSubscriber().Config.WaitTimeout
				}
			} else if waitTime == 0 {
				waitTime = time.Millisecond * 10 //没有订阅者也没有配置发布者等待重连时间，默认10ms后关闭流
			}
			r.resetTimer(waitTime)
			r.Debug("wait publisher", zap.Duration("wait timeout", waitTime))
		case STATE_WAITTRACK:
			if len(r.SEHistory) > 1 {
				stateEvent = SErepublish{event}
			} else {
				stateEvent = SEpublish{event}
			}
			r.resetTimer(time.Second * 5) // 5秒心跳，检测track的存活度
		case STATE_PUBLISHING:
			stateEvent = SEtrackAvaliable{event}
			r.Subscribers.SendInviteTrack(r)
			r.Subscribers.Broadcast(stateEvent)
			if puller, ok := r.Publisher.(IPuller); ok {
				puller.OnConnected()
			}
			r.resetTimer(time.Second * 5) // 5秒心跳，检测track的存活度
		case STATE_WAITCLOSE:
			stateEvent = SEwaitClose{event}
			if r.IdleTimeout > 0 {
				r.resetTimer(r.IdleTimeout)
			} else {
				r.resetTimer(r.DelayCloseTimeout)
			}
		case STATE_CLOSED:
			Streams.Delete(r.Path)
			r.timeout.Stop()
			stateEvent = SEclose{event}
			r.Subscribers.Broadcast(stateEvent)
			r.Tracks.Range(func(_ string, t common.Track) {
				if t.GetPublisher() == nil || t.GetPublisher().GetStream() == r {
					t.Dispose()
				}
			})
			r.Subscribers.Dispose()
			r.actionChan.Close()
		}
		if actionCoust := time.Since(event.Time); actionCoust > 100*time.Millisecond {
			r.Warn("action timeout", zap.String("action", action.String()), zap.Duration("cost", actionCoust))
		}
		EventBus <- stateEvent
		if actionCoust := time.Since(event.Time); actionCoust > 100*time.Millisecond {
			r.Warn("action timeout after eventbus", zap.String("action", action.String()), zap.Duration("cost", actionCoust))
		}
		if r.Publisher != nil {
			r.Publisher.OnEvent(stateEvent)
			if actionCoust := time.Since(event.Time); actionCoust > 100*time.Millisecond {
				r.Warn("action timeout after send to publisher", zap.String("action", action.String()), zap.Duration("cost", actionCoust))
			}
		}
	} else {
		r.Debug("wrong action", zap.String("action", action.String()))
	}
	return
}

func (r *Stream) IsShutdown() bool {
	switch l := len(r.SEHistory); l {
	case 0:
		return false
	case 1:
		return r.SEHistory[0].Action == ACTION_CLOSE
	default:
		switch r.SEHistory[l-1].Action {
		case ACTION_CLOSE:
			return true
		case ACTION_TIMEOUT:
			return r.SEHistory[l-1].From == STATE_WAITCLOSE
		}
	}
	return false
}

func (r *Stream) IsClosed() bool {
	if r == nil {
		return true
	}
	return r.State == STATE_CLOSED
}

func (r *Stream) Close() {
	r.Receive(ACTION_CLOSE)
}

func (s *Stream) Receive(event any) bool {
	if s.IsClosed() {
		return false
	}
	return s.actionChan.Send(event)
}

func (s *Stream) onSuberClose(sub ISubscriber) {
	s.Subscribers.Delete(sub)
	if s.Publisher != nil {
		s.Publisher.OnEvent(sub) // 通知Publisher有订阅者离开，在回调中可以去获取订阅者数量
	}
	// 使用 TotalLen() 确保内部订阅者（如录制）也被计入
	// 只有当所有订阅者（包括内部订阅者）都离开后，才触发 ACTION_LASTLEAVE
	if (s.DelayCloseTimeout > 0 || s.IdleTimeout > 0) && s.Subscribers.TotalLen() == 0 && !sub.GetSubscriber().Config.Internal {
		s.action(ACTION_LASTLEAVE)
	}
}

func (s *Stream) checkRunCost(timeStart time.Time, timeOutInfo zap.Field) {
	if cost := time.Since(timeStart); cost > 100*time.Millisecond {
		s.Warn("run timeout", timeOutInfo, zap.Duration("cost", cost))
	}
}

// 流状态处理中枢，包括接收订阅发布指令等
func (s *Stream) run() {
	EventBus <- SEcreate{StreamEvent{Event[*Stream]{Target: s, Time: time.Now()}}}
	pulseTicker := time.NewTicker(EngineConfig.PulseInterval)
	defer pulseTicker.Stop()
	var timeOutInfo zap.Field
	var timeStart time.Time
	for pulseSuber := make(map[ISubscriber]struct{}); ; s.checkRunCost(timeStart, timeOutInfo) {
		select {
		case <-pulseTicker.C:
			timeStart = time.Now()
			timeOutInfo = zap.String("type", "pulse")
			for sub := range pulseSuber {
				sub.OnEvent(PulseEvent{CreateEvent(struct{}{})})
			}
		case <-s.timeout.C:
			timeStart = time.Now()
			timeOutInfo = zap.String("state", s.State.String())
			if s.State == STATE_PUBLISHING || s.State == STATE_WAITTRACK {
				for sub := range s.Subscribers.internal {
					if sub.IsClosed() {
						delete(s.Subscribers.internal, sub)
						s.Info("innersuber -1", zap.Int("remains", len(s.Subscribers.internal)))
					}
				}
				for sub := range s.Subscribers.public {
					if sub.IsClosed() {
						s.onSuberClose(sub)
					}
				}
				if !s.NeverTimeout {
					lost := false
					trackCount := 0
					timeout := s.PublishTimeout
					if s.IsPause {
						timeout = s.PauseTimeout
					}
					s.Tracks.Range(func(name string, t common.Track) {
						trackCount++
						switch t.(type) {
						case *track.Video, *track.Audio:
							// track 超过一定时间没有更新数据了
							if lastWriteTime := t.LastWriteTime(); !lastWriteTime.IsZero() && time.Since(lastWriteTime) > timeout {
								s.Warn("track timeout", zap.String("name", name), zap.Time("last writetime", lastWriteTime), zap.Duration("timeout", timeout))
								lost = true
							}
						}
					})
					if !lost {
						if trackCount == 0 {
							s.Warn("no tracks")
							if time.Since(s.StartTime) > timeout {
								lost = true
							}
						} else if s.Publisher != nil && s.Publisher.IsClosed() {
							s.Warn("publish is closed", zap.Error(context.Cause(s.publisher)), zap.String("ptr", fmt.Sprintf("%p", s.publisher.Context)))
							lost = true
						}
					}
					if lost {
						s.action(ACTION_TIMEOUT)
						continue
					}
					// 使用 TotalLen() 而非 Len()，确保内部订阅者（如录制）也被计入
					// 避免只有录制订阅者时误触发 ACTION_LASTLEAVE 导致录制中止
					if s.IdleTimeout > 0 && s.Subscribers.TotalLen() == 0 && time.Since(s.StartTime) > s.IdleTimeout {
						s.action(ACTION_LASTLEAVE)
						continue
					}
				}
				switch s.State {
				case STATE_WAITTRACK:
						s.action(ACTION_TRACKAVAILABLE)
				case STATE_WAITCLOSE:
					continue
				}
				s.Subscribers.AbortWait()
				s.resetTimer(time.Second * 5)
			} else {
				s.Debug("timeout", timeOutInfo)
				s.action(ACTION_TIMEOUT)
			}
		case action, ok := <-s.actionChan.C:
			if !ok {
				return
			}
			timeStart = time.Now()
			switch v := action.(type) {
			case SubPulse:
				timeOutInfo = zap.String("action", "SubPulse")
				pulseSuber[v] = struct{}{}
			case *util.Promise[IPublisher]:
				timeOutInfo = zap.String("action", "Publish")
				if s.IsClosed() {
					v.Reject(ErrStreamIsClosed)
					break
				}
				puber := v.Value.GetPublisher()
				oldPuber := s.publisher
				s.publisher = puber
				conf := puber.Config
				republish := s.Publisher == v.Value // 重复发布
				if republish {
					s.Info("republish")
					s.Tracks.Range(func(name string, t common.Track) {
						t.SetStuff(common.TrackStateOffline)
					})
				}
				needKick := !republish && oldPuber != nil && conf.KickExist // 需要踢掉老的发布者
				if needKick {
					s.Warn("kick", zap.String("old type", oldPuber.Type))
					s.Publisher.OnEvent(SEKick{CreateEvent(util.Null)})
				}
				s.Publisher = v.Value
				s.PublishTimeout = conf.PublishTimeout
				s.DelayCloseTimeout = conf.DelayCloseTimeout
				s.IdleTimeout = conf.IdleTimeout
				s.PauseTimeout = conf.PauseTimeout
				if s.action(ACTION_PUBLISH) || republish || needKick {
					if oldPuber != nil {
						// 接管老的发布者的音视频轨道
						puber.AudioTrack = oldPuber.AudioTrack
						puber.VideoTrack = oldPuber.VideoTrack
					}
					v.Resolve()
				} else {
					s.Warn("duplicate publish")
					v.Reject(ErrDuplicatePublish)
				}
			case *util.Promise[ISubscriber]:
				timeOutInfo = zap.String("action", "Subscribe")
				if s.IsClosed() {
					v.Reject(ErrStreamIsClosed)
					break
				}
				suber := v.Value
				io := suber.GetSubscriber()
				sbConfig := io.Config
				waits := &waitTracks{
					Promise: v,
				}
				if ats := io.Args.Get(sbConfig.SubAudioArgName); ats != "" {
					waits.audio.Wait(strings.Split(ats, ",")...)
				} else if len(sbConfig.SubAudioTracks) > 0 {
					waits.audio.Wait(sbConfig.SubAudioTracks...)
				} else if sbConfig.SubAudio {
					waits.audio.Wait()
				}
				if vts := io.Args.Get(sbConfig.SubVideoArgName); vts != "" {
					waits.video.Wait(strings.Split(vts, ",")...)
				} else if len(sbConfig.SubVideoTracks) > 0 {
					waits.video.Wait(sbConfig.SubVideoTracks...)
				} else if sbConfig.SubVideo {
					waits.video.Wait()
				}
				if dts := io.Args.Get(sbConfig.SubDataArgName); dts != "" {
					waits.data.Wait(strings.Split(dts, ",")...)
				} else {
					// waits.data.Wait()
				}
				if s.Publisher != nil {
					s.Publisher.OnEvent(v) // 通知Publisher有新的订阅者加入，在回调中可以去获取订阅者数量
					pubConfig := s.Publisher.GetConfig()
					s.Tracks.Range(func(name string, t common.Track) {
						waits.Accept(t)
					})
					if !pubConfig.PubAudio {
						waits.audio.StopWait()
					} else if s.State == STATE_PUBLISHING && len(waits.audio) > 0 {
						waits.audio.InviteTrack(suber)
					} else if s.Subscribers.waitAborted {
						waits.audio.StopWait()
					}
					if !pubConfig.PubVideo {
						waits.video.StopWait()
					} else if s.State == STATE_PUBLISHING && len(waits.video) > 0 {
						waits.video.InviteTrack(suber)
					} else if s.Subscribers.waitAborted {
						waits.video.StopWait()
					}
				}
				s.Subscribers.Add(suber, waits)
				if s.Subscribers.Len() == 1 && s.State == STATE_WAITCLOSE {
					s.action(ACTION_FIRSTENTER)
				}
			case Unsubscribe:
				timeOutInfo = zap.String("action", "Unsubscribe")
				delete(pulseSuber, v)
				s.onSuberClose(v)
			case TrackRemoved:
				timeOutInfo = zap.String("action", "TrackRemoved")
				if s.IsClosed() {
					break
				}
				name := v.GetName()
				if t, ok := s.Tracks.LoadAndDelete(name); ok {
					s.Info("track -1", zap.String("name", name))
					s.Subscribers.Broadcast(t)
					t.(common.Track).Dispose()
				}
			case *util.Promise[common.Track]:
				timeOutInfo = zap.String("action", "Track")
				if s.IsClosed() {
					v.Reject(ErrStreamIsClosed)
					break
				}
				if s.State == STATE_WAITPUBLISH {
					s.action(ACTION_PUBLISH)
				}
				pubConfig := s.GetPublisherConfig()
				name := v.Value.GetName()
				if _, ok := v.Value.(*track.Video); ok && !pubConfig.PubVideo {
					v.Reject(ErrTrackMute)
					continue
				}
				if _, ok := v.Value.(*track.Audio); ok && !pubConfig.PubAudio {
					v.Reject(ErrTrackMute)
					continue
				}
				if s.Tracks.Add(name, v.Value) {
					v.Resolve()
					s.Subscribers.OnTrack(v.Value)
					if _, ok := v.Value.(*track.Video); ok && !pubConfig.PubAudio {
						s.Subscribers.AbortWait()
					}
					if _, ok := v.Value.(*track.Audio); ok && !pubConfig.PubVideo {
						s.Subscribers.AbortWait()
					}
					if (s.Tracks.MainVideo != nil || !pubConfig.PubVideo) && (!pubConfig.PubAudio || s.Tracks.MainAudio != nil) {
						s.action(ACTION_TRACKAVAILABLE)
					}
				} else {
					v.Reject(ErrBadTrackName)
				}
			case NoMoreTrack:
				s.Subscribers.AbortWait()
			case StreamAction:
				timeOutInfo = zap.String("action", "StreamAction"+v.String())
				s.action(v)
			default:
				timeOutInfo = zap.String("action", "unknown")
				s.Error("unknown action", timeOutInfo)
			}
			if s.IsClosed() && s.actionChan.Close() { //再次尝试关闭
				return
			}
		}
	}
}

func (s *Stream) AddTrack(t common.Track) (promise *util.Promise[common.Track]) {
	promise = util.NewPromise(t)
	if !s.Receive(promise) {
		promise.Reject(ErrStreamIsClosed)
	}
	return
}

func (s *Stream) RemoveTrack(t common.Track) {
	s.Receive(TrackRemoved{t})
}

func (s *Stream) Pause() {
	s.IsPause = true
}

func (s *Stream) Resume() {
	s.IsPause = false
}

type TrackRemoved struct {
	common.Track
}

type SubPulse struct {
	ISubscriber
}

type Unsubscribe ISubscriber
type NoMoreTrack struct{}
