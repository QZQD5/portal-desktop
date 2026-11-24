import { useCallback, useEffect, useRef, useState } from 'react';
import type { VoiceAPI, VoiceAction, VoiceState, VoicePresentation } from '../../shared/voice';
import { VoiceSession } from './session';

const idle: VoiceState = { state: 'stopped', muted: false, inputLevel: 0, error: '', transcript: '', userTranscript: '', userTranscripts: [], task: '' };
export function useVoice(api: VoiceAPI | undefined, scope: string, enabled: boolean, sceneId = '') {
  const session = useRef<VoiceSession | undefined>(undefined);
  const generation = useRef(0);
  // Keep the rendered conversation independent from the short-lived call
  // state. The API/session can be recreated while the chat timeline remains
  // mounted, and closing the call must not erase the last presentation.
  const lastPresentation = useRef<VoicePresentation | undefined>(undefined);
  const [state, setState] = useState(idle);
  useEffect(() => {
    setState(value => ({ ...idle, presentation: value.presentation || lastPresentation.current }));
    return () => { generation.current++; const current = session.current; session.current = undefined; void current?.stop().catch(() => {}); };
  }, [api, scope]);
  const command = useCallback((action: VoiceAction) => {
    if (!api) { setState(value => ({ ...value, state: 'error', error: '桌面语音服务尚未连接，请重启客户端。' })); return; }
    if (action === 'start') {
      if (session.current) return;
      const request = ++generation.current;
      setState({ ...idle, state: 'connecting', microphone: 'starting' });
      let unconfirmedStop: string | undefined;
      const current = new VoiceSession(api, event => {
        if (session.current !== current) return;
        if (event.type === 'desktop.microphone') {
          const microphone = event.state as VoiceState['microphone'];
          setState(value => ({ ...value, microphone }));
        }
        if (event.type === 'desktop.input-level' && typeof event.level === 'number') {
          const inputLevel = event.level;
          setState(value => ({ ...value, inputLevel }));
        }
        if (event.type === 'desktop.presentation') {
          const presentation = event.presentation as VoicePresentation;
          lastPresentation.current = presentation;
          setState(value => ({ ...value, presentation }));
        }
        if (event.type === 'desktop.voice') {
          if (event.state === 'stopped' || event.state === 'error') {
            session.current = undefined;
            void current.stop().catch(() => {});
          }
          if (['stopped', 'connecting', 'ready', 'error'].includes(String(event.state)))
            setState(value => ({ ...value, state: event.state as VoiceState['state'], inputLevel: 0, error: event.state === 'ready' ? '' : value.error }));
        }
        if (event.type === 'error' || (event.type === 'desktop.voice' && event.state === 'error')) {
          unconfirmedStop = (event.error as { code?: string } | undefined)?.code === 'stop_unconfirmed' && typeof event.streamId === 'string'
            ? event.streamId : undefined;
          setState(value => ({ ...value, error: String(event.message || '语音连接异常，请重试。').slice(0, 6000) }));
        }
        if (event.type === 'text.stop.resolved' && unconfirmedStop && event.streamId === unconfirmedStop) {
          unconfirmedStop = undefined;
          setState(value => ({ ...value, error: '' }));
        }
        if (event.type === 'transcript.discard' && event.role === 'user') {
          const id = event.turnId || event.itemId;
          setState(value => {
            const rows = value.userTranscripts || [];
            const remaining = rows.filter(row => row.id !== id || !row.streaming);
            if (remaining.length === rows.length) return value;
            return { ...value, userTranscripts: remaining, userTranscript: remaining.at(-1)?.text || '' };
          });
        }
        if ((event.type === 'transcript.final' || event.type === 'transcript.delta') && event.role === 'user') {
          const content = String(event.content || '');
          setState(value => {
            const id = String(event.turnId || event.itemId || `speech-${Date.now()}`);
            const rows = [...(value.userTranscripts || [])];
            const index = rows.findIndex(row => row.id === id);
            const text = event.replace === true || event.type === 'transcript.final' ? content : (rows[index]?.text || '') + content;
            const row = { id, text: text.slice(0, 6000), streaming: event.type !== 'transcript.final', at: index >= 0 ? rows[index].at : Date.now() };
            if (index >= 0) rows[index] = row; else rows.push(row);
            while (rows.length > 64) rows.shift();
            return { ...value, userTranscript: rows[rows.length - 1].text, userTranscripts: rows, transcript: '' };
          });
        }
        if (event.task && typeof event.task === 'object') {
          const task = event.task as Record<string, unknown>;
          setState(value => ({ ...value, task: String(task.result || task.error || task.message || task.objective || '').slice(0, 6000) }));
        }
      }, sceneId);
      session.current = current;
      void current.start().catch(error => {
        if (generation.current !== request) return;
        session.current = undefined;
        setState(value => ({ ...value, state: 'error', inputLevel: 0, error: String(error.message || '语音连接失败，请重试。').slice(0, 6000) }));
      });
    } else if (action === 'stop') {
      generation.current++;
      const current = session.current; session.current = undefined;
      setState(value => ({ ...idle, presentation: value.presentation || lastPresentation.current }));
      void current?.stop().catch(() => {});
    } else if (action === 'mute') {
      session.current?.mute(!state.muted);
      setState(value => ({ ...value, muted: !value.muted, inputLevel: 0 }));
    } else if (action === 'interrupt') session.current?.interrupt();
  }, [api, enabled, state.muted, sceneId]);
  return { state: api || state.presentation ? state : null, command };
}
