import type { VoiceAction, VoiceState } from '../../shared/voice';
import type { CSSProperties } from 'react';

export function VoiceControl({ state, command }: { state: VoiceState; command(action: VoiceAction): void }) {
  const ready = state.state === 'ready';
  const connecting = state.state === 'connecting';
  const level = ready && !state.muted ? Math.min(1, Math.max(0, state.inputLevel || 0)) : 0;
  const label = connecting ? '取消连接' : ready ? (state.muted ? '恢复收音' : '暂停收音') : '开启语音通话';
  return <div className="composer-voice" data-state={state.state} data-muted={state.muted} data-receiving={level > 0} style={{ '--voice-input-level': level } as CSSProperties}>
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
  if (!active && !state.error) return null;
  const capture = state.microphone ?? 'ready';
  const captureLabel = {
    starting: '正在打开麦克风…', recovering: '麦克风暂不可用，正在重试…',
    unavailable: '麦克风不可用，请检查输入设备', ready: '正在聆听',
  }[capture];
  const label = state.state === 'connecting' ? '正在连接…' : state.muted ? '收音已暂停，回复仍会播放' : captureLabel;
  const userTranscript = state.state === 'ready' && capture === 'ready' && !state.muted && state.userTranscript;
  return <div className="voice-activity">
    {state.state === 'ready' && <button type="button" aria-label="打断回复" title="打断回复" onClick={() => command('interrupt')}>
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" aria-hidden="true"><path d="M4 10v4m4-7v10m4-14v18m4-14v10m4-7v4"/></svg>
    </button>}
    <span className={state.error ? 'voice-error' : ''} role={state.error ? 'alert' : 'status'}>
      {state.error || <>{userTranscript ? <span className="voice-user-transcript">你：{userTranscript}</span> : label}</>}
    </span>
    <button type="button" aria-label={active ? '结束语音通话' : '关闭语音提示'} title={active ? '结束语音通话' : '关闭语音提示'} onClick={() => command('stop')}>
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" aria-hidden="true"><path d="m7 7 10 10M17 7 7 17"/></svg>
    </button>
  </div>;
}
