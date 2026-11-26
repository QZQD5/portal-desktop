import type { VoiceAction, VoiceState } from '../../shared/voice';
import { useEffect, useRef, useState, type CSSProperties } from 'react';

function DictationWaveform({ recording, level }: { recording: boolean; level: number }) {
  const latest = useRef(level);
  useEffect(() => { latest.current = level; }, [level]);
  const [samples, setSamples] = useState<number[]>(() => Array(64).fill(0));
  useEffect(() => {
    if (!recording) return;
    const timer = setInterval(() => {
      const sample = latest.current;
      setSamples(previous => sample || previous.some(Boolean) ? [...previous.slice(1), sample] : previous);
    }, 80);
    return () => clearInterval(timer);
  }, [recording]);
  const path = samples.map((sample, index) => {
    const height = 1 + sample * 19;
    return `M${2 + index * 4} ${(24 - height) / 2}v${height}`;
  }).join(' ');
  return <svg className="dictation-waveform" viewBox="0 0 256 24" preserveAspectRatio="none" aria-hidden="true">
    <path d={path} stroke="currentColor" strokeWidth="2" strokeLinecap="round"/>
  </svg>;
}

export function VoiceControl({ state, command }: { state: VoiceState; command(action: VoiceAction): void }) {
  const ready = state.state === 'ready';
  const connecting = state.state === 'connecting';
  const level = ready && !state.muted ? Math.min(1, Math.max(0, state.inputLevel || 0)) : 0;
  const label = connecting ? '取消连接' : ready ? (state.muted ? '恢复收音' : '暂停收音') : '开启语音通话';
  if (state.dictation) {
    const finishing = state.dictation === 'finishing';
    const recording = !finishing && ready && state.microphone === 'ready';
    const prompt = finishing ? '正在转写…' : connecting ? '正在连接语音…'
      : state.microphone === 'unavailable' ? '麦克风不可用，请检查输入设备'
      : state.microphone === 'recovering' ? '正在重新连接麦克风…'
      : recording ? '请说话…' : '正在打开麦克风…';
    return <div className="composer-dictation" data-recording={recording}>
      <span className="dictation-prompt" role={state.error ? 'alert' : 'status'}>{state.error || prompt}</span>
      <DictationWaveform recording={recording} level={level}/>
      <button type="button" className="dictation-cancel" aria-label="取消听写" title="取消听写" onClick={() => command('stop')}>
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" aria-hidden="true"><path d="m6 6 12 12M18 6 6 18"/></svg>
      </button>
      <button type="button" className="dictation-finish" aria-label="完成听写" title="完成听写" disabled={!recording} onClick={() => command('finish-dictation')}>
        <svg width="19" height="19" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><path d="m5 12 4 4 10-10"/></svg>
      </button>
    </div>;
  }
  return <div className="composer-voice" data-state={state.state} data-muted={state.muted} data-receiving={level > 0} style={{ '--voice-input-level': level } as CSSProperties}>
    {state.dictationAvailable && !ready && !connecting && <button
      type="button" className="dictation-trigger" aria-label="开始听写" title="开始听写" onClick={() => command('dictate')}>
      <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
        <rect x="9" y="2" width="6" height="13" rx="3"/><path d="M5 10v2a7 7 0 0 0 14 0v-2M12 19v3m-3 0h6"/>
      </svg>
    </button>}
    <button type="button" className="voice-trigger" aria-label={label} title={label} aria-pressed={ready && !state.muted} onClick={() => command(connecting ? 'stop' : ready ? 'mute' : 'start')}>
      {/* Original WebUI audio-lines icon (Lucide ISC; see LICENSE.txt). */}
      <svg className="voice-wave" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
        <circle cx="12" cy="12" r="11" fill="currentColor" stroke="none"/>
        <g transform="translate(4 4) scale(.667)" stroke="#4b5563" strokeWidth="2">
          <path d="M2 10v3"/><path d="M6 6v11"/><path d="M10 3v18" stroke="#3979c3"/>
          <path d="M14 8v7"/><path d="M18 5v13"/><path d="M22 10v3"/>
        </g>
        {state.muted && <path d="M5 5 19 19" stroke="#4b5563"/>}
      </svg>
    </button>
  </div>;
}

export function VoiceActivity({ state, command }: { state: VoiceState; command(action: VoiceAction): void }) {
  const active = state.state === 'ready' || state.state === 'connecting';
  if (state.dictation || (!active && !state.error)) return null;
  const capture = state.microphone ?? 'ready';
  const captureLabel = {
    starting: '正在打开麦克风…', recovering: '麦克风暂不可用，正在重试…',
    unavailable: '麦克风不可用，请检查输入设备', ready: '正在聆听',
  }[capture];
  const label = state.state === 'connecting' ? '正在连接…' : state.muted ? '收音已暂停，回复仍会播放' : captureLabel;
  const userTranscript = state.state === 'ready' && capture === 'ready' && !state.muted && state.userTranscript;
  const closeLabel = active ? '结束语音通话' : '关闭语音提示';
  return <div className="voice-activity">
    {state.state === 'ready' && <button type="button" aria-label="打断回复" title="打断回复" onClick={() => command('interrupt')}>
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" aria-hidden="true"><path d="M4 10v4m4-7v10m4-14v18m4-14v10m4-7v4"/></svg>
    </button>}
    <span className={state.error ? 'voice-error' : ''} role={state.error ? 'alert' : 'status'}>
      {state.error || <>{userTranscript ? <span className="voice-user-transcript">你：{userTranscript}</span> : label}</>}
    </span>
    <button type="button" aria-label={closeLabel} title={closeLabel} onClick={() => command('stop')}>
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" aria-hidden="true"><path d="m7 7 10 10M17 7 7 17"/></svg>
    </button>
  </div>;
}
