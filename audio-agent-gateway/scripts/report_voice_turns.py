"""Build a standalone Chinese report from the four outputs of eval_voice_turns.py."""
import argparse
import hashlib
import json
from collections import Counter
from html import escape
from pathlib import Path


def read_rows(path):
    with path.open(encoding="utf-8-sig") as source:
        return [json.loads(line) for line in source if line.strip()]


def table(headers, rows):
    head = "".join(f"<th>{escape(str(h))}</th>" for h in headers)
    body = "".join("<tr>" + "".join(f"<td>{escape(str(v))}</td>" for v in row) + "</tr>" for row in rows)
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


def build(cases_path, directory):
    cases = read_rows(cases_path)
    profiles = {"short": "本地短人声先到", "sustained": "本地持续人声先到", "cloud_first": "云端起声事件先到"}
    modes = {"assigned": "用例指定时序", **profiles}
    triggers = {None: "未清空", "local_sustained": "本地持续人声", "asr_start": "云端起声事件",
                "asr_delta": "临时转写", "asr_final": "最终转写"}
    snapshots = {}
    for key in modes:
        output = read_rows(directory / f"{key}.jsonl")
        if [r["id"] for r in output] != [c["id"] for c in cases]:
            raise ValueError(f"Case IDs or order differ for {key}")
        snapshots[key] = output
    acoustic_cases = read_rows(directory / "corpus-acoustics.jsonl")
    if [r["id"] for r in acoustic_cases] != [c["id"] for c in cases]:
        raise ValueError("Acoustic case IDs differ from the test suite")
    actions = {"KEEP": "继续播", "CUT": "停止播"}
    category_stats = {}
    detail = []
    for i, case in enumerate(cases):
        row = snapshots["assigned"][i]
        expected, actual = case["expected_action"], row["actual_action"]
        label = "命中" if expected == actual else "误停" if expected == "KEEP" else "漏停"
        stats = category_stats.setdefault(case["category"], Counter())
        stats.update([label, expected])
        at = row["first_clear_delta_index"]
        src = triggers.get(row["first_clear_trigger"], row["first_clear_trigger"])
        trigger_text = case["asr_deltas"][at] if at is not None else ""
        signal = acoustic_cases[i]
        onset, gate = signal.get("first_onset_ms"), signal.get("first_playback_gate_ms")
        measurement = (f"开口 {onset} ms；持续门槛 " + (f"{gate} ms" if gate is not None else "未达到")
                       if signal["eval_status"] == "measured" else "跳过，纯噪声或非词声音无法用合成读音代表")
        detail.append({
            "id": case["id"], "category": case["category"], "family": case.get("family", ""),
            "context": case["context"], "text": case["transcript"], "profile": profiles[case["input_profile"]],
            "expected": actions[expected], "actual": actions[actual], "label": label,
            "reason": case.get("rationale", ""), "challenge": case.get("challenge", False),
            "src": src, "triggerText": trigger_text, "deltas": case["asr_deltas"], "acoustic": measurement,
            "replay": "／".join(actions[snapshots[p][i]["actual_action"]] for p in profiles),
        })
    counts = Counter(d["label"] for d in detail)
    strata = Counter((bool(r["challenge"]), r["label"]) for r in detail)
    strata_table = table(["评分组", "命中", "误停", "漏停"],
                         ([name, strata[flag, "命中"], strata[flag, "误停"], strata[flag, "漏停"]]
                          for flag, name in [(False, "常规"), (True, "挑战（含多声源、改口等）")]))
    group_table = table(["场景", "应继续", "应停", "命中", "误停", "漏停"],
                        ([name, s["KEEP"], s["CUT"], s["命中"], s["误停"], s["漏停"]]
                         for name, s in category_stats.items()))
    comparison = []
    for key, rows in snapshots.items():
        pairs = Counter((r["expected_action"], r["actual_action"]) for r in rows)
        comparison.append([modes[key], pairs["KEEP", "KEEP"], pairs["KEEP", "CUT"],
                           pairs["CUT", "CUT"], pairs["CUT", "KEEP"]])
    profile_table = table(["时序", "正确续播", "误停", "正确停播", "漏停"], comparison)
    stopped = Counter(d["src"] for d in detail if d["label"] == "误停")
    trigger_table = table(["首次清空的触发事件", "指定时序中的误停数"], stopped.most_common())
    early = [i for i, r in enumerate(snapshots["assigned"]) if not r["correct"]
             and r["first_clear_trigger"] in ("local_sustained", "asr_start")]
    later = sum(snapshots["short"][i]["first_clear_trigger"] == "asr_delta" for i in early)
    acoustic_rows = read_rows(directory / "acoustics.jsonl")
    acoustic_groups = {}
    group_names = {"stop": "明确停止", "correction": "纠正", "encourage_continuation": "鼓励继续",
                   "backchannel": "附和"}
    for item in acoustic_rows:
        group = group_names[item["category"]] if item["rate"] is not None else "仓库单字夹具"
        acoustic_groups.setdefault(group, []).append(item)
    acoustic_table = table(["合成音频类别", "音频数", "检测到开口", "达到本地硬打断门槛"],
        ([group, len(items), sum(x["first_onset_ms"] is not None for x in items),
          sum(x["first_playback_gate_ms"] is not None for x in items)] for group, items in acoustic_groups.items()))
    per_label = []
    measured = [a for a in acoustic_cases if a["eval_status"] == "measured"]
    for label, display in actions.items():
        subset = [a for a in measured if a["expected_action"] == label]
        per_label.append([display, len(subset), sum(a["first_onset_ms"] is not None for a in subset),
                          sum(a["first_playback_gate_ms"] is not None for a in subset)])
    corpus_table = table(["按用户意图分组", "已测用例", "检测到开口", "达到本地持续门槛"], per_label)
    n = len(cases)
    distinct_text = len({c["transcript"] for c in cases})
    families = len({c["family"] for c in cases})
    evidence_groups = len({c.get("pair_id") for c in cases if c.get("pair_id")})
    root = Path(__file__).resolve().parents[1]
    version_files = [cases_path, root / "gateway/voice.py", root / "gateway/seeduplex.py", root / "gateway/audio_activity.py"]
    hashes = table(["文件", "SHA-256"], ([f.name, hashlib.sha256(f.read_bytes()).hexdigest()] for f in version_files))
    head = f"""<!doctype html><html lang="zh-CN"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>语音插话诊断报告</title><style>
:root{{color-scheme:light}}*{{box-sizing:border-box}}body{{background:#f7f9fa;color:#172c35;font:16px/1.65 system-ui,"Microsoft YaHei",sans-serif;margin:0}}
main{{max-width:1200px;margin:auto;padding:32px 24px 80px}}h1,h2{{line-height:1.3}}h2{{margin-top:36px}}p{{max-width:90ch}}
table{{background:white;border-collapse:collapse;width:100%;margin:12px 0 20px;font-size:14px}}th,td{{padding:7px 12px;text-align:left;border-bottom:1px solid #dce4e7;vertical-align:top}}th{{background:#e8eff1}}.scroll{{overflow:auto}}
select,input{{font:inherit;max-width:100%;padding:4px 8px}}label{{margin-right:12px}}summary{{cursor:pointer;color:#075d69}}.bad{{color:#a4351e;font-weight:650}}small{{color:#40575f}}
</style><main><h1>语音插话诊断报告</h1>
<p>评测对象是已有回复正在播放时，Python Gateway 会继续播放还是发送 <code>playback.clear</code>。
每例有预设的助手上下文、用户转写、增量识别快照、事件顺序和预期动作。命中指控制事件符合用例预期；未测设备出声、云端生成、真实 ASR 或后台工具执行。</p>
<p><strong>{n} 条用例 · {len(category_stats)} 类场景 · {counts['命中']} 命中 · {counts['误停']} 误停 · {counts['漏停']} 漏停</strong><br>
另外将这 {n} 条分别按三种固定时序重放，共 {3*n} 次对照。语料按 {families} 个家族组合生成，含 {distinct_text} 种不同的最终转写，{evidence_groups} 组相同答语对照不同的上一句。各类比例只描述本套用例。真实豆包调用数为 0。</p>
<h2>先看归因</h2>
<p>合成事件经过项目实际的事件归一化、轮次和播放控制代码。误停取第一个真实发出的 <code>playback.clear</code> 为依据。测量窗口从正在播放开始，到本轮最终转写处理完为止；后续能否恢复播放、用户是否听见，以及任务是否取消需要独立验证。</p>
<p>常规和挑战样例分别计分。改口、多声源等挑战项的预期由最终意图和场景说明决定，出现分歧不能直接算作线上缺陷。只有助手原话进入会话，场景旁白和预期动作仅供评测。</p>
{strata_table}
<h2>哪些场景受影响</h2>{group_table}
<h2>同一句话，改变人声持续时间或事件顺序</h2><p>三组对照使用相同的助手原话、增量文本、最终文本和预期动作。本地先到的两组用固定人声分数分别输入 4 帧和 32 帧，每帧 32 ms；云端先到组将起声通知放在 4 帧本地输入之前。各组使用同一批用例，不相加计算独立样本数。</p>{profile_table}
<h2>哪一步清空了播报</h2>{trigger_table}
<p><strong>{len(early)} 条误停在转写到达前就已触发。把这批相同用例改用“本地短人声先到”重放，{later} 条仍在临时转写到达后清空。</strong>
只改起声时机，改善有限。区分“我还在听”与“现在让我说”的规则也必须一起验证。</p>
<p>云端起声和转写均为脚本注入。测试确定 Gateway 在这些输入下的行为；真实豆包是否会先取消自己的生成，仍需独立上游事件记录。</p>
<h2>独立声学验证</h2><p>将用例中可以朗读的文本用 Windows 中文慧慧音色合成 16 kHz 单声道 PCM，输入项目实际的 Silero 模型。覆盖 {len(measured)} 条、{len({a["pcm_sha256"] for a in measured})} 段不同的音频；跳过 {n-len(measured)} 条纯噪声或非词场景。同一段声音可能出现在预期不同的上下文中。时间从文件起点计，包含合成语音前导静音。</p>
{corpus_table}<p>此层只测开口与持续门槛，声音本身没有助手回声或麦克风环境。该实验与上面的控制事件重放独立；没有假设真实模型在哪一帧给出转写。明细与音频摘要见 corpus-acoustics.jsonl。</p>
<p>另外用同一音色、两个语速测 20 个短语，并加入仓库自带的“停”夹具。该组单列用于检查短指令与附和，见 acoustics.jsonl。</p>{acoustic_table}
<h2>据此怎样优化</h2>
<ol>
<li>把“可能要插话”与“终止旧回复”分成两个决定。在本地持续人声或云端起声先到时，先尝试可恢复的降音。用后续转写确认是“别说了”还是“你接着讲”，再决定停播或恢复。优先回归这两条在文字出现前清空的路径。</li>
<li>给短反馈判断提供上一句助手原话。用同一个“好”分别回答“继续讲好吗”和“先停住好吗”的成对用例验证。单独报告明确指令、明确附和和中途改口的效果，并检查等待语义是否使真正的“停”变慢。</li>
<li>用独立测试账号在禁用工具的情况下复核上游。用完全相同的双轨场景跑上游单独对照与完整 Gateway，按同一回复记录上游转写、自然结束／取消、客户端发出的取消和本地清空顺序。上游主动结束且未收到客户端取消、转写本身有误、Gateway 收到正确转写后仍清空，这三种情况分别统计。要进一步区分 Being 代理与供应商模型，还需要同版本同配置的供应商直连对照。</li>
</ol>
<details><summary>如何复跑与核对版本</summary><p>在仓库的 audio-agent-gateway 目录中，用项目 Python 虚拟环境执行。四组控制重放均使用本地模拟传输。测试包内的文件按同一目录结构排列。</p>
<pre style="overflow:auto">python scripts/eval_voice_turns.py tests/fixtures/voice-turn-cases-1000.jsonl -o test-results/voice-turn-eval/assigned.jsonl
foreach ($p in 'short','sustained','cloud_first') {{
    python scripts/eval_voice_turns.py tests/fixtures/voice-turn-cases-1000.jsonl --profile $p -o "test-results/voice-turn-eval/$p.jsonl"
}}
python scripts/eval_voice_acoustics.py
python scripts/eval_voice_acoustics.py --cases tests/fixtures/voice-turn-cases-1000.jsonl
python scripts/report_voice_turns.py tests/fixtures/voice-turn-cases-1000.jsonl test-results/voice-turn-eval</pre>{hashes}</details>
<h2>逐条复核</h2><p>展开用户原话可查看当时助手说了什么、识别快照、第一次清空的触发内容，以及三种事件顺序下的结果。</p>"""
    toolbar = """<label>结论 <select id="status"><option>全部</option><option>误停</option><option>漏停</option><option>命中</option></select></label>
<label>场景 <select id="category"><option>全部</option></select></label>
<label>评分组 <select id="stratum"><option>全部</option><option>常规</option><option>挑战</option></select></label>
<label>搜索 <input id="query" placeholder="用例编号或对话"></label><p id="count"></p>
<div class="scroll"><table><thead><tr><th>编号</th><th>场景</th><th>用户原话与详情</th><th>预期</th><th>观测</th><th>结论</th></tr></thead><tbody id="rows"></tbody></table></div></main>"""
    script = """
<script>
const data=JSON.parse(document.getElementById('records').textContent);
const el=id=>document.getElementById(id);
for(const category of new Set(data.map(row=>row.category))) el('category').add(new Option(category));
function render(){
  const q=el('query').value.trim().toLowerCase(), selected=el('category').value, label=el('status').value;
  const stratum=el('stratum').value;
  const shown=data.filter(r=>(selected==='全部'||selected===r.category)&&(label==='全部'||label===r.label)
    &&(stratum==='全部'||r.challenge===(stratum==='挑战'))
    &&(!q||[r.id,r.context,r.text,r.family].join(' ').toLowerCase().includes(q)));
  el('count').textContent=shown.length+' 条匹配';const frag=document.createDocumentFragment();
  for(const r of shown){
    const tr=document.createElement('tr');
    for(const value of [r.id,r.category]) tr.insertCell().textContent=value;
    const details=document.createElement('details'), summary=document.createElement('summary');
    summary.textContent=r.text||'（空转写）';details.appendChild(summary);tr.insertCell().appendChild(details);
    for(const line of ['助手当时正在说：'+r.context, '预期依据：'+(r.reason||'见用例上下文与标签'),
      '用例时序：'+r.profile, '转写快照：'+JSON.stringify(r.deltas),
      '首次清空：'+r.src+(r.triggerText?' · '+r.triggerText:''),
      '独立合成音频／本地 VAD：'+r.acoustic,
      '短人声／持续人声／云端先到：'+r.replay, '用例家族：'+r.family+(r.challenge?' · 挑战样例':'')]){
      const p=document.createElement('p');p.textContent=line;details.appendChild(p);
    }
    tr.insertCell().textContent=r.expected;tr.insertCell().textContent=r.actual;
    const result=tr.insertCell();result.textContent=r.label;if(r.label!=='命中')result.className='bad';
    frag.appendChild(tr);
  }el('rows').replaceChildren(frag);
}for(const id of ['query','status','category','stratum'])el(id).addEventListener('input',render);render();
</script></html>"""
    blob = json.dumps(detail, ensure_ascii=False, separators=(",", ":")).replace("<", "\\u003c")
    (directory / "index.html").write_text(head + toolbar + '<script type="application/json" id="records">' +
                                          blob + "</script>" + script, encoding="utf-8")
    print(f"{n} cases: {dict(counts)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("cases", type=Path)
    parser.add_argument("results", type=Path)
    args = parser.parse_args()
    build(args.cases, args.results)
