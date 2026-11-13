# Windows 内置 Python 运行时

此目录只构建供 Portal Desktop 启动的 headless Gateway。旧的独立窗口、单文件安装包、WebUI、DSH/Node 内置包和配置文件打包逻辑已移除。

在安装了 `requirements.txt` 和 PyInstaller 的 Python 3.12 环境执行：

```powershell
python -m pip install pyinstaller
python scripts/build_windows.py
```

产物为 `scripts/dist/AudioAgentGateway/` 整个目录，入口是其中的 `AudioAgentGateway.exe`。Desktop 隐藏控制台启动它；不要单独双击。构建不包含任何 Being 或语音密钥。

通常直接在 Portal Desktop 仓库中执行：

```powershell
npm run build:voice -- --source D:\code\audio-agent-gateway --python C:\path\to\python.exe
npm run prepare:desktop
npx electron-forge package --platform=win32 --arch=x64
```

`build:voice` 将运行时复制到 Desktop 的 `resources/audio-gateway/`，并生成包含协议版本和入口 SHA-256 的 `runtime.json`。该目录是生成物，不提交 Git。Gateway 已是文字聊天和本地历史的必需组件，缺少运行时会终止 Desktop 打包。设置 `PORTAL_DESKTOP_GATEWAY_ROOT` 和 `PORTAL_DESKTOP_GATEWAY_PYTHON` 后，`npm run package` / `npm run make` 会自动构建并带入 Gateway，失败则终止打包。当前自动构建脚本只支持 Windows；其他平台必须先准备并验证对应运行时。

数据由 Desktop 按 Being endpoint 和 scene 隔离，位于其用户配置目录的 `voice/` 下。结束语音保留正在执行的任务；退出 Desktop 会保存恢复信息并关闭子进程，重新连接时只回放已关联的 Being stream。
