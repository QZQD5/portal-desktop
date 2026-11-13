# Portal Desktop Python Gateway

Gateway 由 Portal Desktop 作为独立 Python 子进程管理。语音交给豆包 Seeduplex 听和说，豆包把要执行的任务交给 Being。键盘打出的文字直接进入 Being 的对话流，不再调用本机文字模型。语音会话里补发的文字也直接交给 Being。Gateway 保存对话、转发 Being 的回复，并把待播结果交给豆包发声。

Being 派出 Portal 子代理后，Gateway 根据关联的工具回执保留 `delegated` 状态，不把“已委派”当成完成。回调中有可关联的子任务结束回执时，更新原任务状态；只有文字说明时保留待核对。取消会经现有 Being 聊天接口请求 `portal_subagent_control`，取得真实停止回执后才报告取消成功，202、口头确认及网络结果不明均不自动重发。前台文字仍在运行时也会独立接收并归档已发现的回调；Heart 只提供最新流，发现前已被替换的流仍需要服务端历史查询接口才能补取。

## 本地记录与恢复

Desktop 的文字发送、停止、历史读取和流回放都经 Gateway；无需开启麦克风或配置豆包密钥也能使用文字聊天。Desktop 仅保留渲染状态和待发送队列，不再写第二份 IndexedDB 聊天历史；旧 IndexedDB 只读迁入 Gateway，原数据保留。

同场景提交文字（包括流中的补充消息）会打断当前语音生成并清空 Desktop 播放队列，迟到的旧音频不会继续播放；已提交的 Being 后台任务继续执行。重复请求不会再次打断，其他场景的文字不影响当前通话。

Enter 或发送按钮会立即启动文字工具路由，Ctrl+Enter 由 Desktop 本地排队。查询和取消可以在 Being 执行其他任务时进行。文字委派与语音委派按同一个后台队列执行，Being 的正文增量沿用 Desktop 的文字流接口返回。两种委派共用按 Being 端点和场景保存的上下文同步记录，排队期间的新对话留给后续请求。

同步记录只在 Being 返回有效消息流或 202 接收回执后更新，并随 Gateway 状态持久化。失败或未确认的发送不会消耗上下文；并发请求在前一次尚未确认时可能携带重叠背景，迟到回执不会覆盖较新记录。Being 的状态和结果同时保留在共享记录中，豆包在新一轮本地收音开始及云端识别开始时刷新上下文，不依赖结果是否已经播报。

通话期间，文字输入、任务状态和已归档结果同步给豆包作为背景。已完成的 Being 回复进入语音队列，任务状态可通过工具查询。Gateway 保存完整正文，播报和模型背景使用有界文本。语音、文字同时到达时，迟到的最终语音识别仍加入下一次 Being 请求的背景。下一次通话从同场景历史恢复。

Being 负责身份修改及长期记忆的保存、查询、学习和遗忘。明确的语音记忆指令在最终转写后即可提交，其他相关请求由语音模型委派。语音可以直接用近期对话续聊，并用 Heart `/api/status` 返回的显示名自我介绍。Gateway 启动时读取状态，每轮开口也并行查询，语音状态查询最多等待 3 秒。单独问“你是谁”时使用本轮查到的名字回答。Gateway 停用本机长期记忆工具与接口，已有记忆文件留在原处，作为旧数据供用户查阅。

每个 Being 端点使用同一个本地目录，各场景的消息在库中标记 `scene_id`。`history.sqlite3` 保存本机文字、语音转写、任务结果及文字恢复进度；`gateway.sqlite3` 保存语音会话、任务队列、回放日志等运行状态，可以按原保留策略清理。`.runtime` 是开发数据目录，不是另一个 Agent。旧按场景分目录的语音历史会只读迁入新历史库。

Being 网页消息属于 Being 内部记录。委派任务读取 `/api/stream/active`，等待已有工作结束再提交。可关联的 Being 流在断线后从已记录游标恢复，结果按场景归档。升级前已存在的文字流仍可按原游标恢复。

Gateway 每 2 秒发现 Heart 新产生的 `origin: callback` 流。每条回复单独核对 Portal 子任务 ID、Being 回传的原任务标记或可关联的工具回执，再归档和播报。带场景的回调只接收本机已知场景，能关联的共享回调归到原任务场景，无归属的普通回调保留在共享 SBS 时间线。只有收到可关联的子任务结束回执时，才更新原任务状态。游标和完成状态持久化，重启后从已读位置续读。Heart 的回放窗口和 SBS 设置决定可发现的回调。

取消受理时，Gateway 停止交付关联任务的新正文、语音和更新回执。取消记录保留原任务 ID 和已知子任务 ID，任务从列表清理后仍可识别迟到回调。只有正文且缺少任何关联标识的旧回调沿普通回调流程交付。

关闭聊天窗口或退出 Desktop 不会停止已提交的 Being 任务。Gateway 保存本地流游标，重连从已接收位置续读，避免重复文字。若原流已过期、回放有缺口或缺少关联 ID，会保留已收到的内容并报错，不自动重发请求。

默认子进程模式中，Desktop 主进程通过私有 stdin 管道提供 Being 连接、当前 scene、数据目录和随机访问令牌。Gateway 监听 `127.0.0.1` 的随机端口，经 stdout 输出 `gateway.ready`；本机 API 也必须使用 Bearer 令牌，浏览器 Origin 被拒绝。父进程关闭管道或发送换行后，Gateway 保存任务状态并退出，不停止 Being 上已经提交的任务。

Being 使用 Desktop 已保存的连接。语音走 Being 语音代理，使用 Desktop 已配对的 Loom token；不需要单独配置 DOUBAO_API_KEY。

## 两个进程手动启动（开发调试）

安装 Python 3.12 与 `requirements.txt`、Desktop 的 npm 依赖。分别打开两个 PowerShell 终端；路径按本机情况调整。此模式不需要先打包 Gateway。

终端一启动 Python（示例使用当前 Conda 环境）：

```powershell
cd D:\code\audio-agent-gateway
& 'C:\Users\shixi\miniconda3\envs\audio-agent-gateway\python.exe' -u -m gateway --dev --port 3102
```

终端二启动 Desktop 源码：

```powershell
cd C:\Users\shixi\Desktop\portal-desktop
$env:PORTAL_DESKTOP_GATEWAY_URL = 'http://127.0.0.1:3102'
npm start
```

开发时由 Portal Desktop 自动拉起本目录 Gateway。

Gateway 输出 `gateway.ready` 后等待 Being 绑定；Desktop 连接 Being 后通过本机 API 提供绑定，文字聊天与历史读取即会使用 Gateway。点击输入框中发送按钮旁的波形图标开启语音。开发服务器只监听 `127.0.0.1`，拒绝浏览器 Origin，不提供 WebUI；本机程序可直接访问这些开发 API。地址或端口错误会报错，不会自动启动另一份 Gateway。

Python 日志直接显示在终端一。`Ctrl+C` 停止 Gateway；修改 Python 后手动重启，再点击 Desktop 的“开始语音”重连。退出 Desktop 不会结束这个手动启动的进程。IDE 调试入口为模块 `gateway`，参数 `--dev --port 3102`。开发状态保存在 `.runtime/dev/<Being端点摘要>/`，也可用 `--data-dir` 指定绝对目录；不会混用 Desktop 管理的正式数据。自动管理模式使用 Desktop 用户数据目录下的 `gateway/<Being端点摘要>/`。

## Desktop 自动管理子进程

不设置 `PORTAL_DESKTOP_GATEWAY_URL` 时，由 Desktop 启停 Gateway。若需由 Desktop 启动 Python 源码，设置 `PORTAL_DESKTOP_GATEWAY_ROOT` 指向本仓库、`PORTAL_DESKTOP_GATEWAY_PYTHON` 指向该环境的 Python。自动管理的 Gateway 从 Desktop 继承环境变量，VS Code 启动配置只用于调试模式。

构建随 Desktop 分发的 Python 运行时，请参见 `scripts/README.md`。没有给用户直接运行的 Gateway EXE 入口；运行时可执行文件只接受 Desktop 管道配置。

测试：`python -m pytest`。测试使用本机 Being HTTP/SSE 和语音 WebSocket fixture；`tests/test_desktop_milestone.py` 验证真实子进程与手动开发服务器。设置 `GATEWAY_EXECUTABLE` 后，子进程用例改为验证打包运行时。Desktop 的 `npm run test:voice` 验证 Electron 麦克风采集、播放、静音、打断、任务执行和挂断；设置上述源码目录及 Python 环境变量后，`npm run test:voice -- --external` 额外验证手动服务器的重启重连与独立生命周期。

当前运行时自动构建和验收以 Windows 为边界。语音与文字的本机历史已统一；工具授权仍由 Portal Desktop/Being 原有流程处理。文字收到 202 后追踪结果，不重复提交；独立语音任务收到无法关联的 202 时仍保留不自动重发的保护。Gateway 是文字聊天的必需组件，打包缺少它会明确失败；macOS/Linux 运行时构建与分发尚未验收。

## 连续语音请求与受理确认

豆包区分新增查询、修正、查询进度和取消，并调用对应工具。Gateway 按工具执行，
语音委派提交豆包写入工具的任务文本。确认转写只用于判断这轮已经说完，并限制取消范围；任务列表仍取当前场景的活动状态。
取消工具立即返回受理回执，Gateway 在后台发出停止请求，从受理起丢弃目标的后续输出。

“不是桌面，是 D 盘”等纠正使用 `update_agent_task` 更新同一项工作。尚未提交时直接替换
目标，执行中通过 Being 的插话接口发送完整修正和本轮原话。202 表示接收，实际执行状态以回执为准。

## 语音无响应恢复

本地检测到说话后，即使云端没有返回识别开始事件，或仅返回开始事件而没有文字，也会检查识别是否卡住。
用户持续说话时不重置连接；检测到停顿、距说话开始超过 5 秒后，先请求识别结束，再等待 2 秒。
仍没有识别结果时自动重连，并提示连接恢复后重说。静音、休眠和输入暂停期间不会触发此恢复。
没有确认的转写不会用于执行任务。

已经取得文字并提交播报的回复使用独立音频截止时间：`speech_first_audio_timeout` 默认 15 秒，
`speech_audio_inactivity_timeout` 默认 20 秒。收到响应 ID 或文字更新不延长首段音频等待；
播报开始后，只有新音频延长音频停滞截止时间。超时后清除旧播放、恢复语音连接，
后台任务保持原状态；这些截止时间不用于限制 Being 查找或执行工具的耗时。
日志 `realtime.speech_stalled` 的 `reason` 区分 `first_audio_timeout` 和 `audio_inactivity_timeout`。
修改源码需重启 Gateway 生效；模拟服务回归不代表真实通话已验收。
