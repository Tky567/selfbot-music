if (!globalThis.WebSocket) {
    globalThis.WebSocket = require('ws');
}

require('dotenv').config();
const { Client } = require('discord.js-selfbot-v13');
const {
    joinVoiceChannel,
    createAudioPlayer,
    createAudioResource,
    entersState,
    VoiceConnectionStatus,
    AudioPlayerStatus,
    NoSubscriberBehavior,
    StreamType,
} = require('@discordjs/voice');
const { execSync, execFile, spawn } = require('child_process');
const { promisify } = require('util');
const execFileAsync = promisify(execFile);
const path = require('path');
const fs = require('fs');

const isWindows = process.platform === 'win32';

if (isWindows) {
    process.env.FFMPEG_PATH = path.join(__dirname, 'bin', 'ffmpeg.exe');
} else {
    // On Linux, use system ffmpeg from PATH
    try {
        const systemFfmpeg = execSync('which ffmpeg', { timeout: 5000 }).toString().trim();
        process.env.FFMPEG_PATH = systemFfmpeg;
        console.log(`[Init] Using system ffmpeg: ${systemFfmpeg}`);
    } catch {
        // Fallback to bundled binary
        process.env.FFMPEG_PATH = path.join(__dirname, 'bin', 'ffmpeg');
        console.log('[Init] System ffmpeg not found, using bundled bin/ffmpeg');
    }
}
const YTDLP_PATH = path.join(__dirname, 'bin', isWindows ? 'yt-dlp.exe' : 'yt-dlp');

if (!isWindows) {
    try {
        fs.chmodSync(YTDLP_PATH, 0o755);
        console.log('[Init] Granted execute permission for yt-dlp');
    } catch (e) {
        console.error('[Init] Cannot grant execute permission for yt-dlp:', e.message);
    }
}

try {
    const ffmpegVer = execSync(`"${process.env.FFMPEG_PATH}" -version`, { timeout: 10000 }).toString().split('\n')[0].trim();
    console.log(`[Init] FFmpeg: ${ffmpegVer}`);
} catch (e) {
    console.error(`[Init] FFmpeg is not working! Error: ${e.message}`);
}

try {
    const ver = execSync(`"${YTDLP_PATH}" --version`, { timeout: 10000 }).toString().trim();
    console.log(`[Init] yt-dlp version: ${ver}`);
} catch (e) {
    console.error(`[Init] yt-dlp is not working! Error: ${e.message}`);
    console.error('[Init] Make sure bin/yt-dlp is the correct binary for the current OS (' + process.platform + ')');
}

let HAS_NODE = false;
try {
    const nodeVer = execSync('node --version', { timeout: 5000 }).toString().trim();
    HAS_NODE = true;
    console.log(`[Init] Node runtime detected for yt-dlp JS: ${nodeVer}`);
} catch (e) {
    console.warn('[Init] Node runtime not detected for yt-dlp JS runtime fallback.');
}

let HAS_DENO = false;
try {
    const denoVer = execSync('deno --version', { timeout: 5000 }).toString().split('\n')[0].trim();
    HAS_DENO = true;
    console.log(`[Init] Optional Deno runtime detected for yt-dlp JS: ${denoVer}`);
} catch (e) {
    console.log('[Init] Deno not found. Continuing with Node/default yt-dlp JS runtime.');
}

let YTDLP_JS_RUNTIME_ARGS = [];
if (HAS_NODE) {
    YTDLP_JS_RUNTIME_ARGS = ['--no-js-runtimes', '--js-runtimes', 'node'];
    console.log('[Init] yt-dlp JS runtime forced to: node');
} else if (HAS_DENO) {
    YTDLP_JS_RUNTIME_ARGS = ['--no-js-runtimes', '--js-runtimes', 'deno'];
    console.log('[Init] yt-dlp JS runtime fallback: deno');
} else {
    console.warn('[Init] No external JS runtime found for yt-dlp; using yt-dlp defaults only.');
}

function withYtdlpRuntimeArgs(args = []) {
    return [...YTDLP_JS_RUNTIME_ARGS, ...args];
}

function runDetached(task, label) {
    Promise.resolve()
        .then(task)
        .catch((error) => {
            console.error(`[Detached Task Error:${label}]`, error);
        });
}

function parseYouTubeUrl(rawUrl) {
    let parsed;
    try {
        parsed = new URL(rawUrl);
    } catch {
        return null;
    }

    const validHosts = ['www.youtube.com', 'youtube.com', 'm.youtube.com', 'music.youtube.com', 'youtu.be'];
    if (!validHosts.includes(parsed.hostname)) {
        return null;
    }

    return parsed.toString();
}

async function ensureVoiceConnection(message, guildId, voiceChannelId) {
    let connection = activeConnection;
    const shouldCreateConnection =
        !connection ||
        connection.joinConfig.guildId !== guildId;

    if (shouldCreateConnection) {
        if (connection) {
            try {
                connection.destroy();
            } catch (e) {
            }
        }

        connection = joinVoiceChannel({
            guildId,
            channelId: voiceChannelId,
            adapterCreator: message.guild.voiceAdapterCreator,
            selfDeaf: true,
            selfMute: false,
            debug: true,
        });
    } else if (connection.joinConfig.channelId !== voiceChannelId) {
        try {
            connection.rejoin({
                channelId: voiceChannelId,
                selfDeaf: false,
                selfMute: false,
            });
        } catch (e) {
            try {
                connection.destroy();
            } catch (e2) {
            }
            connection = joinVoiceChannel({
                guildId,
                channelId: voiceChannelId,
                adapterCreator: message.guild.voiceAdapterCreator,
                selfDeaf: false,
                selfMute: false,
                debug: true,
            });
        }
    }

    activeConnection = connection;
    await entersState(connection, VoiceConnectionStatus.Ready, 30_000);
    return connection;
}

process.on('unhandledRejection', (err) => {
    console.error('[UnhandledRejection]', err);
});
process.on('uncaughtException', (err) => {
    console.error('[UncaughtException]', err);
});

const client = new Client();

let isStreaming = false;
let currentVideoUrl = '';
let lastVideoUrl = '';
let activeSessionId = 0;
let activeConnection = null;
let activePlayer = null;
let activeYtdlpProcess = null;
let activeFfmpegProcess = null;
let latestPlayRequestId = 0;

const PREFIX = '!';
const ADMIN_IDS = (process.env.ADMIN_ID || '').split(',').map(id => id.trim()).filter(Boolean);

const COOKIE_PATH = path.join(__dirname, 'cookies.txt');
const HAS_COOKIE = fs.existsSync(COOKIE_PATH);
if (HAS_COOKIE) {
    console.log('[Init] Found cookies.txt');
    if (!isWindows) {
        try {
            const raw = fs.readFileSync(COOKIE_PATH, 'utf8');
            if (raw.includes('\r\n')) {
                fs.writeFileSync(COOKIE_PATH, raw.replace(/\r\n/g, '\n'), 'utf8');
                console.log('[Init] Fixed line endings in cookies.txt (\\r\\n -> \\n) for Linux');
            }
        } catch (e) {
            console.error('[Init] Cannot fix line endings in cookies.txt:', e.message);
        }
    }
}

async function getMediaInfo(url) {
    const baseArgs = ['--dump-single-json', '--no-playlist', '--no-warnings'];
    const parseInfo = (stdout) => {
        const info = JSON.parse(stdout);
        return {
            title: info.title || 'Unknown title',
            duration: Number.isFinite(info.duration) ? info.duration : 0,
            isLive: !!info.is_live,
        };
    };

    try {
        const { stdout } = await execFileAsync(YTDLP_PATH, withYtdlpRuntimeArgs([...baseArgs, url]));
        return parseInfo(stdout);
    } catch (e) {
        console.error('[yt-dlp getMediaInfo] Attempt 1 failed:', e.stderr || e.message);
        if (HAS_COOKIE) {
            try {
                const { stdout } = await execFileAsync(YTDLP_PATH, withYtdlpRuntimeArgs([...baseArgs, '--cookies', COOKIE_PATH, url]));
                return parseInfo(stdout);
            } catch (e2) {
                console.error('[yt-dlp getMediaInfo] Attempt 2 (cookie) failed:', e2.stderr || e2.message);
            }
        }
    }

    return {
        title: 'Unknown title',
        duration: 0,
        isLive: false,
    };
}

function spawnYtdlpAudio(url) {
    const args = [
        '-f', 'bestaudio/best',
        '--no-playlist',
        '--no-warnings',
        '--no-part',
        '--extractor-args', 'youtube:player_client=tv_embedded',
        ...(HAS_COOKIE ? ['--cookies', COOKIE_PATH] : []),
        '-o', '-',
        url,
    ];

    const proc = spawn(YTDLP_PATH, withYtdlpRuntimeArgs(args), {
        stdio: ['ignore', 'pipe', 'pipe'],
    });

    proc.stderr.on('data', (chunk) => {
        const msg = chunk.toString().trim();
        if (msg && !msg.startsWith('[download]')) {
            console.log('[yt-dlp audio]', msg);
        }
    });

    return proc;
}

function spawnFfmpegOpusFromStdin() {
    const ffmpegArgs = [
        '-hide_banner',
        '-loglevel', 'warning',
        '-i', 'pipe:0',
        '-vn',
        '-c:a', 'libopus',
        '-b:a', '128k',
        '-ar', '48000',
        '-ac', '2',
        '-f', 'ogg',
        'pipe:1',
    ];

    const proc = spawn(process.env.FFMPEG_PATH, ffmpegArgs, {
        stdio: ['pipe', 'pipe', 'pipe'],
    });

    proc.stderr.on('data', (chunk) => {
        const msg = chunk.toString().trim();
        if (msg) {
            console.log('[ffmpeg audio]', msg);
        }
    });

    return proc;
}

function killProcessSafe(proc) {
    if (!proc || proc.killed) return;
    try {
        proc.kill();
    } catch (e) {
    }
}

function stopStreaming(options = {}) {
    const { leaveVoice = false } = options;

    activeSessionId += 1;
    isStreaming = false;
    currentVideoUrl = '';

    if (activePlayer) {
        try {
            activePlayer.stop(true);
        } catch (e) {
        }
        activePlayer = null;
    }

    if (leaveVoice && activeConnection) {
        try {
            activeConnection.destroy();
        } catch (e) {
        }
        activeConnection = null;
    }

    killProcessSafe(activeYtdlpProcess);
    killProcessSafe(activeFfmpegProcess);
    activeYtdlpProcess = null;
    activeFfmpegProcess = null;
}

function waitForPlaybackEnd(player, sessionId) {
    return new Promise((resolve, reject) => {
        const onIdle = () => {
            cleanup();
            resolve('idle');
        };

        const onError = (err) => {
            cleanup();
            reject(err);
        };

        const onStateChange = (_, newState) => {
            if (newState.status === AudioPlayerStatus.Idle) {
                onIdle();
            }
        };

        const interval = setInterval(() => {
            if (sessionId !== activeSessionId) {
                cleanup();
                resolve('cancelled');
            }
        }, 500);

        const cleanup = () => {
            clearInterval(interval);
            player.off('error', onError);
            player.off('stateChange', onStateChange);
        };

        player.on('error', onError);
        player.on('stateChange', onStateChange);
    });
}

function waitForPlaybackStart(player, sessionId, timeoutMs = 12_000) {
    return new Promise((resolve, reject) => {
        const onError = (err) => {
            cleanup();
            reject(err);
        };

        const onStateChange = (_, newState) => {
            if (newState.status === AudioPlayerStatus.Playing) {
                cleanup();
                resolve('playing');
            }
        };

        const cancelCheck = setInterval(() => {
            if (sessionId !== activeSessionId) {
                cleanup();
                resolve('cancelled');
            }
        }, 200);

        const timeout = setTimeout(() => {
            cleanup();
            resolve('timeout');
        }, timeoutMs);

        const cleanup = () => {
            clearInterval(cancelCheck);
            clearTimeout(timeout);
            player.off('error', onError);
            player.off('stateChange', onStateChange);
        };

        player.on('error', onError);
        player.on('stateChange', onStateChange);
    });
}

async function startAudioPlayback(message, guildId, voiceChannelId, videoUrl) {
    const sessionId = ++activeSessionId;
    isStreaming = true;
    currentVideoUrl = videoUrl;

    try {
        const connection = await ensureVoiceConnection(message, guildId, voiceChannelId);

        // Spawn yt-dlp + ffmpeg immediately in parallel with voice join
        const ytProc = spawnYtdlpAudio(videoUrl);
        const ffmpegProc = spawnFfmpegOpusFromStdin();
        activeYtdlpProcess = ytProc;
        activeFfmpegProcess = ffmpegProc;

        ytProc.on('error', (err) => {
            console.error('[yt-dlp audio process error]', err.message);
        });
        ffmpegProc.on('error', (err) => {
            console.error('[ffmpeg audio process error]', err.message);
        });
        ffmpegProc.stdin.on('error', (err) => {
            if (err && err.code !== 'EPIPE') {
                console.error('[ffmpeg stdin error]', err.message);
            }
        });
        ytProc.stdout.on('error', (err) => {
            if (err && err.code !== 'EPIPE') {
                console.error('[yt-dlp stdout error]', err.message);
            }
        });
        ffmpegProc.stdout.on('error', (err) => {
            if (err && err.code !== 'EPIPE') {
                console.error('[ffmpeg stdout error]', err.message);
            }
        });

        ytProc.stdout.pipe(ffmpegProc.stdin);
        ytProc.on('close', () => {
            if (!ffmpegProc.stdin.destroyed) {
                ffmpegProc.stdin.end();
            }
        });

        const player = createAudioPlayer({
            behaviors: {
                noSubscriber: NoSubscriberBehavior.Pause,
            },
        });
        activePlayer = player;

        const resource = createAudioResource(ffmpegProc.stdout, {
            inputType: StreamType.OggOpus,
            inlineVolume: false,
        });

        // Handle voice connection errors and disconnects during playback
        connection.on('error', (err) => {
            console.error('[Voice Connection Error]', err.message);
        });
        connection.on('stateChange', async (oldState, newState) => {
            if (sessionId !== activeSessionId) return;
            if (newState.status === VoiceConnectionStatus.Disconnected) {
                try {
                    // Try to reconnect within 5 seconds
                    await entersState(connection, VoiceConnectionStatus.Connecting, 5_000);
                } catch {
                    // If reconnect fails, clean up
                    console.error('[Voice] Disconnected and could not reconnect, stopping playback.');
                    if (sessionId === activeSessionId) {
                        stopStreaming();
                    }
                }
            } else if (newState.status === VoiceConnectionStatus.Destroyed) {
                // Connection was destroyed externally
                if (sessionId === activeSessionId) {
                    stopStreaming();
                }
            }
        });

        // Wait for voice ready, then start playback immediately
        await entersState(connection, VoiceConnectionStatus.Ready, 20_000);
        connection.subscribe(player);
        player.play(resource);

        // Stop can arrive before Playing. Exit fast when session changes to avoid long pending waits.
        const startState = await waitForPlaybackStart(player, sessionId, 12_000);
        if (startState === 'cancelled') return;
        if (startState !== 'playing') {
            throw new Error('Audio player did not reach Playing state in time.');
        }

        // Fetch metadata only after playback actually starts to avoid extra yt-dlp load on quick stop.
        runDetached(async () => {
            const info = await getMediaInfo(videoUrl);
            if (sessionId !== activeSessionId || !isStreaming) return;
            const durationLabel = info.isLive
                ? 'Live'
                : `${Math.floor(info.duration / 60)}m${info.duration % 60}s`;
            await message.channel.send(`🎵 **Now playing (voice):** ${info.title} | ${durationLabel}`).catch(() => {});
        }, 'metadata');

        const endReason = await waitForPlaybackEnd(player, sessionId);
        if (endReason === 'cancelled') return;
        if (sessionId !== activeSessionId || !isStreaming) return;

        await message.channel.send('✅ Audio playback finished. Dùng `!leave` để bot rời voice.');
        stopStreaming();
    } catch (error) {
        if (sessionId === activeSessionId) {
            console.error('[Audio System Error]', error);
            stopStreaming();
            await message.channel.send('❌ An error occurred during audio playback.').catch(() => {});
        }
    }
}

client.on('ready', () => {
    console.log('-----------------------------------------');
    console.log('YouTube Voice Player Ready!');
    console.log(`User: ${client.user.tag}`);
    console.log('Mode: Audio-only (voice channel)');
    console.log('Commands: !join, !play, !stop, !leave, !help');
    console.log('-----------------------------------------');
});

client.on('messageCreate', async (message) => {
    if (message.author.bot) return;
    if (ADMIN_IDS.length > 0 && !ADMIN_IDS.includes(message.author.id)) return;

    const args = message.content.trim().split(/\s+/);
    const command = (args[0] || '').toLowerCase();

    if (command === `${PREFIX}help`) {
        message.reply(
            '**COMMAND LIST:**\n' +
            '1️⃣ `!join`: Join your current voice channel.\n' +
            '2️⃣ `!play <youtube_link>`: Play audio from YouTube in voice channel.\n' +
            '3️⃣ `!stop`: Stop current playback but stay in voice channel.\n' +
            '4️⃣ `!leave`: Leave the voice channel.\n' +
            '*Note: Only YouTube links are supported.*'
        );
        return;
    }

    if (command === `${PREFIX}join`) {
        const guildId = message.guildId;

        if (!guildId) {
            return message.reply('❌ This command can only be used in a server.');
        }

        message.reply('⏳ Joining your voice channel...');
        runDetached(async () => {
            const member = await message.guild.members.fetch(message.author.id).catch(() => null);
            const voiceId = member?.voice?.channelId;

            if (!voiceId) {
                await message.channel.send('🎙️ Please join a voice channel before using !join.').catch(() => {});
                return;
            }

            try {
                await ensureVoiceConnection(message, guildId, voiceId);
                await message.channel.send('✅ Joined your voice channel.').catch(() => {});
            } catch (error) {
                console.error('[Join Error]', error);
                await message.channel.send('❌ Failed to join voice channel.').catch(() => {});
            }
        }, 'join');
        return;
    }

    if (command === `${PREFIX}play`) {
        const inputUrl = args.slice(1).join(' ').trim();
        const guildId = message.guildId;

        if (!guildId) {
            return message.reply('❌ This command can only be used in a server.');
        }

        const normalizedInputUrl = inputUrl ? parseYouTubeUrl(inputUrl) : null;
        if (inputUrl && !normalizedInputUrl) {
            return message.reply('❌ Only valid YouTube links are supported.');
        }

        const videoUrl = normalizedInputUrl || lastVideoUrl;
        if (!videoUrl) {
            return message.reply('❓ Please provide a YouTube link (Example: `!play https://youtube.com/...`) or use !help.');
        }

        const requestId = ++latestPlayRequestId;
        lastVideoUrl = videoUrl;

        if (isStreaming) {
            stopStreaming();
        }

        if (!inputUrl && lastVideoUrl) {
            message.reply('🔁 No URL provided, replaying the last YouTube link...');
        } else {
            message.reply('⏳ Processing audio for voice playback...');
        }

        runDetached(async () => {
            if (requestId !== latestPlayRequestId) {
                return;
            }

            const member = await message.guild.members.fetch(message.author.id).catch(() => null);
            const voiceId = member?.voice?.channelId;

            if (requestId !== latestPlayRequestId) {
                return;
            }

            if (!voiceId) {
                await message.channel.send('🎙️ Please join a voice channel before using !play.').catch(() => {});
                return;
            }

            await startAudioPlayback(message, guildId, voiceId, videoUrl);
        }, 'play');
        return;
    }

    if (command === `${PREFIX}stop`) {
        latestPlayRequestId += 1;
        message.reply('⏹️ Stopped playback. Bot is still in voice channel.');
        runDetached(async () => {
            stopStreaming();
        }, 'stop');
        return;
    }

    if (command === `${PREFIX}leave`) {
        latestPlayRequestId += 1;
        message.reply('👋 Left the voice channel.');
        runDetached(async () => {
            stopStreaming({ leaveVoice: true });
        }, 'leave');
        return;
    }
});

if (!process.env.DISCORD_TOKEN || ADMIN_IDS.length === 0) {
    console.error('Missing DISCORD_TOKEN or ADMIN_ID in .env');
    process.exit(1);
}

client.login(process.env.DISCORD_TOKEN);
