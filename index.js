import express from 'express';
import crypto from 'crypto';
import WebSocket from 'ws';
import fs from 'fs';
import { exec } from 'child_process';
import { promisify } from 'util';
import querystring from 'querystring';
import dotenv from 'dotenv';
import fetch from 'node-fetch';

dotenv.config();

const execAsync = promisify(exec);
const app = express();
app.use(express.json());

//=============================================================================
// CONFIGURATION SECTION
//=============================================================================

const CONFIG = {
    // Real-time transcription settings
    REALTIME: {
        ENABLED: process.env.REALTIME_ENABLED !== 'false', // Default: true
        MODE: process.env.REALTIME_MODE || 'mixed', // 'mixed' or 'individual'
    },
    
    // Audio channel settings
    AUDIO: {
        CHANNELS: process.env.AUDIO_CHANNELS || 'mono', // 'mono' or 'multichannel'
        SAMPLE_RATE: parseInt(process.env.AUDIO_SAMPLE_RATE) || 16000,
        TARGET_CHUNK_DURATION_MS: parseInt(process.env.TARGET_CHUNK_DURATION_MS) || 100,
    },
    
    // Async transcription settings
    ASYNC: {
        ENABLED: process.env.ASYNC_ENABLED !== 'false', // Default: true
    }
};

// Async transcription configuration
// Full configuration options available at: https://www.assemblyai.com/docs/api-reference/transcripts/submit
const ASYNC_CONFIG = {
    speaker_labels: false,
    // auto_chapters: true,
    // redact_pii: true,
};

// Environment variables
const ZOOM_SECRET_TOKEN = process.env.ZOOM_SECRET_TOKEN;
const CLIENT_ID = process.env.ZM_CLIENT_ID;
const CLIENT_SECRET = process.env.ZM_CLIENT_SECRET;
const ASSEMBLYAI_API_KEY = process.env.ASSEMBLYAI_API_KEY;

console.log('🎧 Zoom RTMS to AssemblyAI Transcription Service');
console.log('📋 Configuration:');
console.log(`   Real-time: ${CONFIG.REALTIME.ENABLED ? '✅' : '❌'} (${CONFIG.REALTIME.MODE})`);
console.log(`   Audio: ${CONFIG.AUDIO.CHANNELS} @ ${CONFIG.AUDIO.SAMPLE_RATE}Hz`);
console.log(`   Async: ${CONFIG.ASYNC.ENABLED ? '✅' : '❌'}`);
console.log('CLIENT_ID:', CLIENT_ID);

// Audio streaming constants
const SAMPLE_RATE = CONFIG.AUDIO.SAMPLE_RATE;
const CHANNELS = CONFIG.AUDIO.CHANNELS === 'multichannel' ? 2 : 1;
const BYTES_PER_SAMPLE = 2;
const TARGET_CHUNK_SIZE = (SAMPLE_RATE * CHANNELS * BYTES_PER_SAMPLE * CONFIG.AUDIO.TARGET_CHUNK_DURATION_MS) / 1000;

// RTMS Protocol Constants
const AUDIO_SAMPLE_RATES = {
    SR_8K: 0,
    SR_16K: 1,
    SR_32K: 2,
    SR_48K: 3
};

const AUDIO_CHANNELS_ENUM = {
    MONO: 1,
    STEREO: 2
};

const AUDIO_CODECS = {
    L16: 1,
    G711: 2,
    G722: 3,
    OPUS: 4
};

const MEDIA_DATA_OPTIONS = {
    AUDIO_MIXED_STREAM: 1,
    AUDIO_MULTI_STREAMS: 2,
};

// Active connections and audio collectors
const activeConnections = new Map();
const audioCollectors = new Map();
const participantStreams = new Map(); // For individual participant transcription

// AssemblyAI Streaming Configuration
const CONNECTION_PARAMS = {
  sample_rate: SAMPLE_RATE,
  format_turns: true,
};
const API_ENDPOINT_BASE_URL = "wss://streaming.assemblyai.com/v3/ws";
const API_ENDPOINT = `${API_ENDPOINT_BASE_URL}?${querystring.stringify(CONNECTION_PARAMS)}`;

//=============================================================================
// ASYNC TRANSCRIPTION FUNCTIONS
//=============================================================================

async function uploadFileToAssemblyAI(wavFilename) {
    try {
        console.log(`📤 Uploading ${wavFilename} to AssemblyAI...`);
        
        const audioData = fs.readFileSync(wavFilename);
        
        const uploadResponse = await fetch('https://api.assemblyai.com/v2/upload', {
            method: 'POST',
            headers: {
                'Authorization': ASSEMBLYAI_API_KEY,
                'Content-Type': 'application/octet-stream'
            },
            body: audioData
        });
        
        if (!uploadResponse.ok) {
            throw new Error(`Upload failed: ${uploadResponse.status} ${uploadResponse.statusText}`);
        }
        
        const uploadResult = await uploadResponse.json();
        console.log(`✅ File uploaded successfully`);
        
        return uploadResult.upload_url;
    } catch (error) {
        console.error(`❌ Error uploading file: ${error.message}`);
        throw error;
    }
}

async function startTranscription(audioUrl, meetingUuid) {
    try {
        console.log(`🎯 Starting async transcription job for ${meetingUuid}...`);
        
        const requestBody = {
            audio_url: audioUrl,
            ...ASYNC_CONFIG, // Spread the entire config object
        };
        
        // Add multichannel support if configured
        if (CONFIG.AUDIO.CHANNELS === 'multichannel') {
            requestBody.multichannel = true;
        }
        
        const transcriptResponse = await fetch('https://api.assemblyai.com/v2/transcript', {
            method: 'POST',
            headers: {
                'Authorization': ASSEMBLYAI_API_KEY,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(requestBody)
        });
        
        const responseText = await transcriptResponse.text();
        
        if (!transcriptResponse.ok) {
            console.error(`❌ Response: ${responseText}`);
            throw new Error(`Transcription start failed: ${transcriptResponse.status} - ${responseText}`);
        }
        
        const transcriptResult = JSON.parse(responseText);
        console.log(`🎬 Transcription job started with ID: ${transcriptResult.id}`);
        
        return transcriptResult.id;
    } catch (error) {
        console.error(`❌ Error starting transcription: ${error.message}`);
        throw error;
    }
}

async function waitForTranscription(transcriptId, meetingUuid) {
    const maxAttempts = 120;
    let attempts = 0;
    
    console.log(`⏳ Waiting for transcription ${transcriptId} to complete...`);
    
    while (attempts < maxAttempts) {
        try {
            const statusResponse = await fetch(`https://api.assemblyai.com/v2/transcript/${transcriptId}`, {
                headers: {
                    'Authorization': ASSEMBLYAI_API_KEY
                }
            });
            
            if (!statusResponse.ok) {
                throw new Error(`Status check failed: ${statusResponse.status} ${statusResponse.statusText}`);
            }
            
            const transcript = await statusResponse.json();
            
            if (transcript.status === 'completed') {
                console.log(`✅ Async transcription completed for ${meetingUuid}`);
                return transcript;
            } else if (transcript.status === 'error') {
                throw new Error(`Transcription failed: ${transcript.error}`);
            }
            
            console.log(`🔄 Status: ${transcript.status}, checking again in 5 seconds...`);
            await new Promise(resolve => setTimeout(resolve, 5000));
            attempts++;
            
        } catch (error) {
            console.error(`❌ Error checking transcription status: ${error.message}`);
            attempts++;
            await new Promise(resolve => setTimeout(resolve, 5000));
        }
    }
    
    throw new Error('Transcription timeout - took longer than 10 minutes');
}

async function performAsyncTranscription(wavFilename, meetingUuid) {
    if (!CONFIG.ASYNC.ENABLED) {
        console.log(`⏭️ Async transcription disabled, skipping for ${meetingUuid}`);
        return;
    }
    
    try {
        console.log(`📄 Starting async transcription for ${meetingUuid}`);
        
        if (!fs.existsSync(wavFilename)) {
            throw new Error(`WAV file does not exist: ${wavFilename}`);
        }
        
        const fileStats = fs.statSync(wavFilename);
        console.log(`📊 WAV file size: ${fileStats.size} bytes`);
        
        if (fileStats.size === 0) {
            throw new Error(`WAV file is empty: ${wavFilename}`);
        }
        
        const audioUrl = await uploadFileToAssemblyAI(wavFilename);
        const transcriptId = await startTranscription(audioUrl, meetingUuid);
        const transcript = await waitForTranscription(transcriptId, meetingUuid);
        
        console.log("✅ Async transcription completed:");
        console.log(`📝 Text: ${transcript.text?.substring(0, 200)}...`);
        
        const transcriptFilename = `transcript_${meetingUuid.replace(/[^a-zA-Z0-9]/g, '_')}.json`;
        fs.writeFileSync(transcriptFilename, JSON.stringify(transcript, null, 2));
        console.log(`💾 Full transcript saved: ${transcriptFilename}`);
        
        const textFilename = `transcript_${meetingUuid.replace(/[^a-zA-Z0-9]/g, '_')}.txt`;
        fs.writeFileSync(textFilename, transcript.text || 'No transcript text available');
        console.log(`💾 Text transcript saved: ${textFilename}`);
        
        if (transcript.summary) {
            console.log(`📋 Summary: ${transcript.summary}`);
        }
        
        if (transcript.utterances && transcript.utterances.length > 0) {
            const speakers = [...new Set(transcript.utterances.map(u => u.speaker))];
            console.log(`👥 Detected speakers: ${speakers.join(', ')}`);
        }
        
        return transcript;
        
    } catch (error) {
        console.error(`❌ Async transcription error: ${error.message}`);
        throw error;
    }
}

//=============================================================================
// WEBHOOK HANDLER
//=============================================================================

app.post('/webhook', async (req, res) => {
    const { event, payload } = req.body;

    if (event === 'endpoint.url_validation' && payload?.plainToken) {
        const hash = crypto
            .createHmac('sha256', ZOOM_SECRET_TOKEN)
            .update(payload.plainToken)
            .digest('hex');
        console.log('✅ URL validation successful');
        return res.json({
            plainToken: payload.plainToken,
            encryptedToken: hash,
        });
    }

    if (event === 'meeting.rtms_started') {
        console.log(`🎤 Meeting started: ${payload.meeting_uuid}`);
        const { meeting_uuid, rtms_stream_id, server_urls } = payload;
        
        initializeAudioCollection(meeting_uuid);
        if (CONFIG.REALTIME.ENABLED) {
            initializeAssemblyAIStreaming(meeting_uuid);
        }
        connectToZoomRTMS(meeting_uuid, rtms_stream_id, server_urls);
    }

    if (event === 'meeting.rtms_stopped') {
        console.log(`🛑 Meeting stopped: ${payload.meeting_uuid}`);
        const { meeting_uuid } = payload;
        await cleanupMeeting(meeting_uuid);
    }

    res.sendStatus(200);
});

//=============================================================================
// AUDIO COLLECTION AND STREAMING
//=============================================================================

function initializeAudioCollection(meetingUuid) {
    const collector = {
        audioChunks: [],
        audioBuffer: [],
        totalBytes: 0,
        chunkCount: 0,
        startTime: Date.now(),
        streamingWs: null,
        stopRequested: false,
        participantChannels: new Map(), // For multichannel audio
    };
    
    audioCollectors.set(meetingUuid, collector);
    
    if (CONFIG.REALTIME.MODE === 'individual') {
        participantStreams.set(meetingUuid, new Map());
    }
}

function initializeAssemblyAIStreaming(meetingUuid) {
    const collector = audioCollectors.get(meetingUuid);
    if (!collector) return;

    if (CONFIG.REALTIME.MODE === 'mixed') {
        // Single stream for all participants
        initializeSingleStream(meetingUuid, collector);
    } else if (CONFIG.REALTIME.MODE === 'individual') {
        // Will create streams per participant as they're detected
        console.log(`🔗 Ready for individual participant streams for meeting ${meetingUuid}`);
    }
}

function initializeSingleStream(meetingUuid, collector) {
    console.log(`🔗 Connecting to AssemblyAI streaming for meeting ${meetingUuid}`);

    const streamingWs = new WebSocket(API_ENDPOINT, {
        headers: {
            Authorization: ASSEMBLYAI_API_KEY,
        },
    });

    collector.streamingWs = streamingWs;

    streamingWs.on('open', () => {
        console.log(`✅ AssemblyAI streaming connected for meeting ${meetingUuid}`);
    });

    streamingWs.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            handleAssemblyAIMessage(data, meetingUuid, 'mixed');
        } catch (error) {
            console.error(`❌ AssemblyAI message error: ${error}`);
        }
    });

    streamingWs.on('error', (error) => {
        console.error(`❌ AssemblyAI streaming error: ${error}`);
        collector.stopRequested = true;
    });

    streamingWs.on('close', (code, reason) => {
        console.log(`🔌 AssemblyAI streaming closed: ${code} - ${reason}`);
    });
}

function initializeParticipantStream(meetingUuid, participantId) {
    console.log(`🔗 Creating individual stream for participant ${participantId} in meeting ${meetingUuid}`);

    const streamingWs = new WebSocket(API_ENDPOINT, {
        headers: {
            Authorization: ASSEMBLYAI_API_KEY,
        },
    });

    const participantStreamsForMeeting = participantStreams.get(meetingUuid);
    participantStreamsForMeeting.set(participantId, {
        ws: streamingWs,
        buffer: [],
        stopRequested: false
    });

    streamingWs.on('open', () => {
        console.log(`✅ AssemblyAI streaming connected for participant ${participantId}`);
    });

    streamingWs.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            handleAssemblyAIMessage(data, meetingUuid, 'individual', participantId);
        } catch (error) {
            console.error(`❌ AssemblyAI message error for participant ${participantId}: ${error}`);
        }
    });

    streamingWs.on('error', (error) => {
        console.error(`❌ AssemblyAI streaming error for participant ${participantId}: ${error}`);
        const participantStream = participantStreamsForMeeting.get(participantId);
        if (participantStream) {
            participantStream.stopRequested = true;
        }
    });

    streamingWs.on('close', (code, reason) => {
        console.log(`🔌 AssemblyAI streaming closed for participant ${participantId}: ${code} - ${reason}`);
    });

    return streamingWs;
}

function handleAssemblyAIMessage(data, meetingUuid, mode, participantId = null) {
    const msgType = data.type;
    const prefix = mode === 'individual' ? `[Participant ${participantId}]` : `[${meetingUuid.substring(0, 8)}]`;

    if (msgType === "Begin") {
        console.log(`🚀 AssemblyAI session started: ${prefix}`);
    } else if (msgType === "Turn") {
        const transcript = data.transcript || "";
        const formatted = data.turn_is_formatted;

        if (formatted) {
            process.stdout.write('\r' + ' '.repeat(100) + '\r');
            console.log(`📝 ${prefix} FINAL: ${transcript}`);
        } else {
            process.stdout.write(`\r🎙️ ${prefix} ${transcript}`);
        }
    } else if (msgType === "Termination") {
        console.log(`\n🏁 AssemblyAI session terminated: ${prefix}`);
    }
}

//=============================================================================
// ZOOM RTMS CONNECTION
//=============================================================================

function generateSignature(CLIENT_ID, meetingUuid, streamId, CLIENT_SECRET) {
    const message = `${CLIENT_ID},${meetingUuid},${streamId}`;
    return crypto.createHmac('sha256', CLIENT_SECRET).update(message).digest('hex');
}

function connectToZoomRTMS(meetingUuid, streamId, serverUrl) {
    console.log(`📡 Connecting to Zoom signaling for meeting ${meetingUuid}`);

    const ws = new WebSocket(serverUrl);

    if (!activeConnections.has(meetingUuid)) {
        activeConnections.set(meetingUuid, {});
    }
    activeConnections.get(meetingUuid).signaling = ws;

    ws.on('open', () => {
        console.log(`✅ Zoom signaling connected for meeting ${meetingUuid}`);
        const signature = generateSignature(CLIENT_ID, meetingUuid, streamId, CLIENT_SECRET);

        const handshake = {
            msg_type: 1,
            protocol_version: 1,
            meeting_uuid: meetingUuid,
            rtms_stream_id: streamId,
            sequence: Math.floor(Math.random() * 1e9),
            signature,
            media_type: 1,
            media_params: {
                audio: {
                    data_opt: CONFIG.REALTIME.MODE === 'individual' ? 
                        MEDIA_DATA_OPTIONS.AUDIO_MULTI_STREAMS : 
                        MEDIA_DATA_OPTIONS.AUDIO_MIXED_STREAM,
                }
            }
        };
        
        console.log(`🎛️ Audio mode: ${CONFIG.REALTIME.MODE === 'individual' ? 'Multi-streams' : 'Mixed stream'}`);
        ws.send(JSON.stringify(handshake));
    });

    ws.on('message', (data) => {
        const msg = JSON.parse(data);

        if (msg.msg_type === 2 && msg.status_code === 0) {
            const mediaUrl = msg.media_server?.server_urls?.audio || msg.media_server?.server_urls?.all;
            if (mediaUrl) {
                connectToZoomMedia(mediaUrl, meetingUuid, streamId, ws);
            }
        }

        if (msg.msg_type === 12) {
            ws.send(JSON.stringify({
                msg_type: 13,
                timestamp: msg.timestamp,
            }));
        }
    });

    ws.on('error', (err) => {
        console.error(`❌ Zoom signaling error: ${err}`);
    });

    ws.on('close', () => {
        console.log(`🔌 Zoom signaling closed for meeting ${meetingUuid}`);
    });
}

function connectToZoomMedia(mediaUrl, meetingUuid, streamId, signalingSocket) {
    console.log(`🎵 Connecting to Zoom media for meeting ${meetingUuid}`);

    const mediaWs = new WebSocket(mediaUrl, { rejectUnauthorized: false });

    if (activeConnections.has(meetingUuid)) {
        activeConnections.get(meetingUuid).media = mediaWs;
    }

    mediaWs.on('open', () => {
        console.log(`✅ Zoom media connected for meeting ${meetingUuid}`);
        const signature = generateSignature(CLIENT_ID, meetingUuid, streamId, CLIENT_SECRET);
        
        const handshake = {
            msg_type: 3,
            protocol_version: 1,
            meeting_uuid: meetingUuid,
            rtms_stream_id: streamId,
            signature,
            media_type: 1,
            payload_encryption: false,
            media_params: {
                audio: {
                    content_type: 2, // RAW_AUDIO
                    sample_rate: SAMPLE_RATE === 16000 ? AUDIO_SAMPLE_RATES.SR_16K : AUDIO_SAMPLE_RATES.SR_8K,
                    channel: CONFIG.AUDIO.CHANNELS === 'multichannel' ? AUDIO_CHANNELS_ENUM.STEREO : AUDIO_CHANNELS_ENUM.MONO,
                    codec: AUDIO_CODECS.L16,
                    data_opt: CONFIG.REALTIME.MODE === 'individual' ? 
                        MEDIA_DATA_OPTIONS.AUDIO_MULTI_STREAMS : 
                        MEDIA_DATA_OPTIONS.AUDIO_MIXED_STREAM,
                    send_rate: 100
                }
            }
        };
        
        mediaWs.send(JSON.stringify(handshake));
    });

    mediaWs.on('message', (data) => {
        try {
            const msg = JSON.parse(data.toString());

            if (msg.msg_type === 4 && msg.status_code === 0) {
                signalingSocket.send(JSON.stringify({
                    msg_type: 7,
                    rtms_stream_id: streamId,
                }));
                console.log(`🚀 Started audio streaming for meeting ${meetingUuid}`);
                
                const dataOpt = msg.media_params?.audio?.data_opt;
                if (dataOpt !== undefined) {
                    console.log(`🎤 Confirmed audio mode: ${dataOpt === 2 ? 'MULTI-STREAMS' : 'MIXED'}`);
                }
            }

            if (msg.msg_type === 12) {
                mediaWs.send(JSON.stringify({
                    msg_type: 13,
                    timestamp: msg.timestamp,
                }));
            }

            if (msg.msg_type === 14 && msg.content?.data) {
                const participantId = msg.content.user_id !== undefined ? msg.content.user_id : 0;
                handleAudioData(msg.content.data, meetingUuid, participantId);
            }

        } catch (err) {
            console.log('📦 Received non-JSON data (unexpected)');
        }
    });

    mediaWs.on('error', (err) => {
        console.error(`❌ Zoom media error: ${err}`);
    });

    mediaWs.on('close', () => {
        console.log(`🔌 Zoom media closed for meeting ${meetingUuid}`);
    });
}

//=============================================================================
// AUDIO DATA HANDLING
//=============================================================================

function handleAudioData(base64Data, meetingUuid, participantId) {
    const collector = audioCollectors.get(meetingUuid);
    if (!collector || collector.stopRequested) return;

    const audioBuffer = Buffer.from(base64Data, 'base64');
    
    // Store for post-meeting processing
    collector.audioChunks.push(audioBuffer);
    collector.totalBytes += audioBuffer.length;
    collector.chunkCount++;

    // Handle multichannel storage
    if (CONFIG.AUDIO.CHANNELS === 'multichannel') {
        if (!collector.participantChannels.has(participantId)) {
            collector.participantChannels.set(participantId, []);
        }
        collector.participantChannels.get(participantId).push(audioBuffer);
    }

    // Send to appropriate AssemblyAI stream(s)
    if (CONFIG.REALTIME.ENABLED) {
        if (CONFIG.REALTIME.MODE === 'mixed') {
            sendToAssemblyAI(audioBuffer, meetingUuid);
        } else if (CONFIG.REALTIME.MODE === 'individual') {
            sendToParticipantStream(audioBuffer, meetingUuid, participantId);
        }
    }

    // Log progress
    if (collector.chunkCount % 100 === 0) {
        const duration = (Date.now() - collector.startTime) / 1000;
        console.log(`🎵 [${meetingUuid.substring(0, 8)}] ${collector.chunkCount} chunks, ${collector.totalBytes} bytes, ${duration.toFixed(1)}s`);
    }
}

function sendToAssemblyAI(audioData, meetingUuid) {
    const collector = audioCollectors.get(meetingUuid);
    if (!collector || !collector.streamingWs || collector.stopRequested) return;

    collector.audioBuffer.push(audioData);
    
    const totalBufferedSize = collector.audioBuffer.reduce((sum, chunk) => sum + chunk.length, 0);
    
    if (totalBufferedSize >= TARGET_CHUNK_SIZE) {
        const combinedBuffer = Buffer.concat(collector.audioBuffer);
        const chunkToSend = combinedBuffer.subarray(0, TARGET_CHUNK_SIZE);
        const remainingData = combinedBuffer.subarray(TARGET_CHUNK_SIZE);
        
        collector.audioBuffer = remainingData.length > 0 ? [remainingData] : [];
        
        if (collector.streamingWs.readyState === WebSocket.OPEN) {
            try {
                collector.streamingWs.send(chunkToSend);
            } catch (error) {
                console.error(`❌ Error sending to AssemblyAI: ${error}`);
            }
        }
    }
}

function sendToParticipantStream(audioData, meetingUuid, participantId) {
    const participantStreamsForMeeting = participantStreams.get(meetingUuid);
    if (!participantStreamsForMeeting) return;

    let participantStream = participantStreamsForMeeting.get(participantId);
    
    // Create stream for new participant
    if (!participantStream) {
        initializeParticipantStream(meetingUuid, participantId);
        participantStream = participantStreamsForMeeting.get(participantId);
    }

    if (!participantStream || participantStream.stopRequested) return;

    participantStream.buffer.push(audioData);
    
    const totalBufferedSize = participantStream.buffer.reduce((sum, chunk) => sum + chunk.length, 0);
    
    if (totalBufferedSize >= TARGET_CHUNK_SIZE) {
        const combinedBuffer = Buffer.concat(participantStream.buffer);
        const chunkToSend = combinedBuffer.subarray(0, TARGET_CHUNK_SIZE);
        const remainingData = combinedBuffer.subarray(TARGET_CHUNK_SIZE);
        
        participantStream.buffer = remainingData.length > 0 ? [remainingData] : [];
        
        if (participantStream.ws && participantStream.ws.readyState === WebSocket.OPEN) {
            try {
                participantStream.ws.send(chunkToSend);
            } catch (error) {
                console.error(`❌ Error sending to AssemblyAI for participant ${participantId}: ${error}`);
            }
        }
    }
}

function flushAudioBuffer(meetingUuid) {
    const collector = audioCollectors.get(meetingUuid);
    if (!collector || collector.audioBuffer.length === 0) return;

    const combinedBuffer = Buffer.concat(collector.audioBuffer);
    const minChunkSize = (SAMPLE_RATE * CHANNELS * BYTES_PER_SAMPLE * 50) / 1000;
    
    if (combinedBuffer.length >= minChunkSize && collector.streamingWs?.readyState === WebSocket.OPEN) {
        try {
            collector.streamingWs.send(combinedBuffer);
            console.log(`🔄 Flushed remaining audio for meeting ${meetingUuid}`);
        } catch (error) {
            console.error(`❌ Error flushing audio: ${error}`);
        }
    }
    
    collector.audioBuffer = [];
}

function flushParticipantStreams(meetingUuid) {
    const participantStreamsForMeeting = participantStreams.get(meetingUuid);
    if (!participantStreamsForMeeting) return;

    for (const [participantId, participantStream] of participantStreamsForMeeting.entries()) {
        if (participantStream.buffer.length > 0) {
            const combinedBuffer = Buffer.concat(participantStream.buffer);
            const minChunkSize = (SAMPLE_RATE * CHANNELS * BYTES_PER_SAMPLE * 50) / 1000;
            
            if (combinedBuffer.length >= minChunkSize && participantStream.ws?.readyState === WebSocket.OPEN) {
                try {
                    participantStream.ws.send(combinedBuffer);
                    console.log(`🔄 Flushed remaining audio for participant ${participantId}`);
                } catch (error) {
                    console.error(`❌ Error flushing audio for participant ${participantId}: ${error}`);
                }
            }
            
            participantStream.buffer = [];
        }
    }
}

//=============================================================================
// AUDIO FILE CREATION
//=============================================================================

async function saveAudioFile(meetingUuid, audioChunks, participantChannels) {
    try {
        const baseName = `recording_${meetingUuid.replace(/[^a-zA-Z0-9]/g, '_')}`;
        
        if (CONFIG.AUDIO.CHANNELS === 'multichannel' && participantChannels.size > 0) {
            return await createMultichannelAudioFile(baseName, participantChannels);
        } else {
            return await createSingleChannelAudioFile(baseName, audioChunks);
        }
    } catch (error) {
        console.error(`❌ Error saving audio: ${error}`);
        throw error;
    }
}

async function createSingleChannelAudioFile(baseName, audioChunks) {
    const rawFilename = `${baseName}.raw`;
    const wavFilename = `${baseName}.wav`;

    try {
        const combinedBuffer = Buffer.concat(audioChunks);
        fs.writeFileSync(rawFilename, combinedBuffer);

        const command = `ffmpeg -y -f s16le -ar ${SAMPLE_RATE} -ac ${CHANNELS} -i ${rawFilename} ${wavFilename}`;
        await execAsync(command);
        
        // Clean up raw file immediately after conversion
        if (fs.existsSync(rawFilename)) {
            fs.unlinkSync(rawFilename);
        }

        console.log(`💾 Single-channel audio saved: ${wavFilename}`);
        return wavFilename;
    } catch (error) {
        // Clean up raw file on error too
        if (fs.existsSync(rawFilename)) {
            try {
                fs.unlinkSync(rawFilename);
            } catch (cleanupError) {
                console.error(`❌ Error cleaning up raw file: ${cleanupError.message}`);
            }
        }
        throw error;
    }
}

async function createMultichannelAudioFile(baseName, participantChannels) {
    console.log(`🎛️ Creating multichannel audio file with ${participantChannels.size} participants`);
    
    const participants = Array.from(participantChannels.entries()).slice(0, 2); // Limit to 2 channels
    const wavFilename = `${baseName}_multichannel.wav`;
    const tempFiles = [];
    
    try {
        if (participants.length === 1) {
            // Single participant, create mono file
            const [participantId, chunks] = participants[0];
            const rawFilename = `${baseName}_temp.raw`;
            const combinedBuffer = Buffer.concat(chunks);
            fs.writeFileSync(rawFilename, combinedBuffer);
            
            const command = `ffmpeg -y -f s16le -ar ${SAMPLE_RATE} -ac 1 -i ${rawFilename} ${wavFilename}`;
            await execAsync(command);
            
            // Clean up temp file
            if (fs.existsSync(rawFilename)) {
                fs.unlinkSync(rawFilename);
            }
            
            console.log(`💾 Single participant audio saved: ${wavFilename}`);
            return wavFilename;
        }
        
        // Create separate files for each participant
        for (let i = 0; i < participants.length; i++) {
            const [participantId, chunks] = participants[i];
            const tempRawFile = `${baseName}_temp_${participantId}.raw`;
            const tempWavFile = `${baseName}_temp_${participantId}.wav`;
            
            const combinedBuffer = Buffer.concat(chunks);
            fs.writeFileSync(tempRawFile, combinedBuffer);
            
            const command = `ffmpeg -y -f s16le -ar ${SAMPLE_RATE} -ac 1 -i ${tempRawFile} ${tempWavFile}`;
            await execAsync(command);
            
            // Clean up raw file immediately
            if (fs.existsSync(tempRawFile)) {
                fs.unlinkSync(tempRawFile);
            }
            
            tempFiles.push(tempWavFile);
        }
        
        // Merge into stereo file
        if (tempFiles.length === 2) {
            const mergeCommand = `ffmpeg -y -i ${tempFiles[0]} -i ${tempFiles[1]} -filter_complex "[0:a][1:a]amerge=inputs=2[a]" -map "[a]" ${wavFilename}`;
            await execAsync(mergeCommand);
        } else {
            // Single file, just copy it
            fs.copyFileSync(tempFiles[0], wavFilename);
        }
        
        console.log(`💾 Multichannel audio saved: ${wavFilename}`);
        return wavFilename;
        
    } catch (error) {
        console.error(`❌ Error creating multichannel audio: ${error.message}`);
        throw error;
    } finally {
        // Clean up temp files
        tempFiles.forEach(file => {
            if (fs.existsSync(file)) {
                try {
                    fs.unlinkSync(file);
                } catch (cleanupError) {
                    console.error(`❌ Error cleaning up temp file ${file}: ${cleanupError.message}`);
                }
            }
        });
    }
}

//=============================================================================
// CLEANUP FUNCTIONS
//=============================================================================

async function cleanupMeeting(meetingUuid) {
    const collector = audioCollectors.get(meetingUuid);
    if (!collector) return;

    console.log(`🧹 Cleaning up meeting ${meetingUuid}`);
    
    collector.stopRequested = true;
    
    if (CONFIG.REALTIME.ENABLED) {
        if (CONFIG.REALTIME.MODE === 'mixed') {
            flushAudioBuffer(meetingUuid);
        } else if (CONFIG.REALTIME.MODE === 'individual') {
            flushParticipantStreams(meetingUuid);
        }
    }
    
    // Close AssemblyAI connections
    if (collector.streamingWs) {
        try {
            if (collector.streamingWs.readyState === WebSocket.OPEN) {
                collector.streamingWs.send(JSON.stringify({ type: "Terminate" }));
            }
            setTimeout(() => {
                if (collector.streamingWs) {
                    collector.streamingWs.close();
                }
            }, 1000);
        } catch (error) {
            console.error(`❌ Error closing AssemblyAI streaming: ${error}`);
        }
    }
    
    // Close individual participant streams
    const participantStreamsForMeeting = participantStreams.get(meetingUuid);
    if (participantStreamsForMeeting) {
        for (const [participantId, participantStream] of participantStreamsForMeeting.entries()) {
            try {
                if (participantStream.ws && participantStream.ws.readyState === WebSocket.OPEN) {
                    participantStream.ws.send(JSON.stringify({ type: "Terminate" }));
                    participantStream.ws.close();
                }
            } catch (error) {
                console.error(`❌ Error closing participant ${participantId} stream: ${error}`);
            }
        }
        participantStreams.delete(meetingUuid);
    }

    // Close Zoom connections
    if (activeConnections.has(meetingUuid)) {
        const connections = activeConnections.get(meetingUuid);
        for (const conn of Object.values(connections)) {
            if (conn && typeof conn.close === 'function') {
                conn.close();
            }
        }
        activeConnections.delete(meetingUuid);
    }

    // Save audio file and perform async transcription
    if (collector.audioChunks.length > 0) {
        try {
            const wavFilename = await saveAudioFile(meetingUuid, collector.audioChunks, collector.participantChannels);
            
            if (CONFIG.ASYNC.ENABLED) {
                await performAsyncTranscription(wavFilename, meetingUuid);
            }
            
            // Clean up WAV file
            try {
                fs.unlinkSync(wavFilename);
                console.log(`🗑️ Cleaned up ${wavFilename}`);
            } catch (cleanupError) {
                console.error(`❌ Error cleaning up WAV file: ${cleanupError.message}`);
            }
            
        } catch (error) {
            console.error(`❌ Error in post-meeting processing: ${error}`);
        }
    }

    audioCollectors.delete(meetingUuid);
}

//=============================================================================
// SERVER STARTUP
//=============================================================================

process.on('SIGINT', () => {
    console.log('\n🛑 Shutting down...');
    for (const [meetingUuid] of audioCollectors.entries()) {
        cleanupMeeting(meetingUuid);
    }
    setTimeout(() => process.exit(0), 2000);
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
    console.log(`🚀 Server running on port ${PORT}`);
    console.log(`📡 Webhook endpoint: http://localhost:${PORT}/webhook`);
    console.log('Ready for Zoom RTMS transcription!');
});