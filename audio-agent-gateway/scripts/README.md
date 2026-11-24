# Windows 和 macOS 内置 Gateway 运行时

此目录只构建供 Portal Desktop 启动的 headless Gateway。旧的独立窗口、单文件安装包、WebUI、DSH/Node 内置包和配置文件打包逻辑已移除。

在安装了 `requirements.txt` 和 PyInstaller 的 Python 3.12 环境执行：

```powershell
python -m pip install pyinstaller
python scripts/build_windows.py
```

产物为 `scripts/dist/AudioAgentGateway/` 整个目录。Windows 入口是 `AudioAgentGateway.exe`，macOS 入口是 `AudioAgentGateway`。Desktop 启动子进程后通过标准输入传入会话配置。

通常直接在 Portal Desktop 仓库中执行：

```powershell
npm run build:voice -- --source D:\code\audio-agent-gateway --python C:\path\to\python.exe
npm run prepare:desktop
npx electron-forge package --platform=win32 --arch=x64
```

`npm run package` 和 `npm run make` 会构建与当前平台和架构匹配的 Gateway，复制到 `resources/audio-gateway/`，并将入口 SHA-256 写入 `runtime.json`。用 `PORTAL_DESKTOP_GATEWAY_PYTHON` 选择安装了依赖的 Python。macOS 发布在 Intel 和 Apple Silicon 上分别构建，使用 conda-forge 的 Python 3.12 和 PyTorch CPU 包；PyInstaller 用应用的签名身份签署原生组件。`npm run test:packaged-runtime` 会启动已打包的 Gateway，检查健康接口和聊天历史。

数据由 Desktop 按 Being endpoint 和 scene 隔离，位于其用户配置目录的 `voice/` 下。结束语音保留正在执行的任务；退出 Desktop 会保存恢复信息并关闭子进程，重新连接时只回放已关联的 Being stream。
