# Zoom RTMS to AssemblyAI Transcription Service

A powerful Node.js service that connects Zoom's Real-Time Media Streaming (RTMS) to AssemblyAI for live transcription and post-meeting analysis. Supports both real-time streaming transcription and comprehensive async transcription with advanced AI features.

## 🌟 Features

### Real-Time Transcription
- **Live streaming transcription** during Zoom meetings
- **Two modes**: Mixed stream (all participants) or Individual streams (per participant)
- **Sub-second latency** from speech to text
- **Speaker identification** and labeling

### Audio Processing
- **Flexible audio handling**: Mono or multichannel recording
- **Multiple participant support** with separate channel recording
- **High-quality audio processing** at 16kHz sample rate
- **Automatic audio file generation** in WAV format

### Async Transcription & AI Features
- **Comprehensive transcription** after meeting ends
- **Speaker diarization** with automatic labeling
- **AI-powered summarization** with customizable formats
- **Sentiment analysis** throughout the conversation
- **Entity detection** (names, locations, organizations)
- **Auto-formatting** with punctuation and capitalization

### Configuration & Control
- **Environment-based configuration** for easy deployment
- **Modular feature toggles** for real-time and async processing
- **Configurable audio parameters** and processing options
- **Production-ready** with proper error handling and cleanup

## 🚀 Quick Start

### Prerequisites
- **Node.js 18+** 
- **ffmpeg** installed and available in PATH
- **Zoom Developer Account** with RTMS enabled
- **AssemblyAI API Key**

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd zoom-rtms-assemblyai
   ```

2. **Install dependencies**
   ```bash
   npm install
   ```

3. **Configure environment**
   ```bash
   cp .env.example .env
   # Edit .env with your credentials
   ```

4. **Start the service**
   ```bash
   npm start
   ```

## ⚙️ Configuration

### Required Environment Variables

```env
# AssemblyAI Configuration
ASSEMBLYAI_API_KEY=your_assemblyai_api_key_here

# Zoom RTMS Configuration  
ZM_CLIENT_ID=your_zoom_client_id_here
ZM_CLIENT_SECRET=your_zoom_client_secret_here
ZOOM_SECRET_TOKEN=your_zoom_secret_token_here
```

### Optional Configuration

```env
# Real-time Transcription
REALTIME_ENABLED=true                    # Enable/disable real-time transcription
REALTIME_MODE=mixed                      # 'mixed' or 'individual'

# Audio Processing
AUDIO_CHANNELS=mono                      # 'mono' or 'multichannel'
AUDIO_SAMPLE_RATE=16000                 # Sample rate in Hz
TARGET_CHUNK_DURATION_MS=100            # Audio chunk size

# Async Transcription
ASYNC_ENABLED=true                       # Enable/disable async transcription

# Server
PORT=8080                               # Server port
NODE_ENV=development                    # Environment mode
```

### Advanced AI Configuration

Edit the `ASYNC_CONFIG` object in `index.js` for detailed control:

```javascript
const ASYNC_CONFIG = {
    speaker_labels: true,
    summarization: true,
    summary_type: "bullets",
    summary_model: "informative", 
    sentiment_analysis: true,
    entity_detection: true,
    punctuate: true,
    format_text: true,
    // Add more options from AssemblyAI docs
};
```

See [AssemblyAI API Reference](https://www.assemblyai.com/docs/api-reference/transcripts/submit) for all available options.

## 🎛️ Operating Modes

### Real-Time Transcription Modes

#### Mixed Mode (Default)
- All participants transcribed in a single stream
- Shows combined conversation flow
- Lower resource usage
- Best for general meeting transcription

```env
REALTIME_MODE=mixed
```

#### Individual Mode
- Each participant gets a separate transcription stream
- Real-time speaker identification
- Higher accuracy for multi-speaker scenarios
- More resource intensive

```env
REALTIME_MODE=individual
```

### Audio Channel Modes

#### Mono (Default)
- Single channel audio recording
- Mixed audio from all participants
- Smaller file sizes
- Standard transcription processing

```env
AUDIO_CHANNELS=mono
```

#### Multichannel
- Separate audio channels per participant
- Enables speaker-specific audio analysis
- Better quality for complex conversations
- Requires multichannel-capable processing

```env
AUDIO_CHANNELS=multichannel
```

## 📋 Zoom Setup

### 1. Create Zoom App
1. Go to [Zoom Marketplace](https://marketplace.zoom.us/)
2. Create a new **General App**
3. Fill in basic app information

### 2. Configure RTMS
1. Enable **Real-Time Media Streaming (RTMS)**
2. Set webhook URL: `https://your-domain.com/webhook`
3. Enable webhook events:
   - `meeting.rtms_started`
   - `meeting.rtms_stopped` 
   - `endpoint.url_validation`

### 3. Get Credentials
- **Client ID**: From your Zoom app dashboard
- **Client Secret**: From your Zoom app dashboard  
- **Secret Token**: Generate in RTMS webhook settings

## 🏗️ System Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Zoom Meeting  │    │  RTMS Service    │    │   AssemblyAI    │
│                 │────│                  │────│                 │
│ • Audio Stream  │    │ • WebSocket      │    │ • Real-time     │
│ • Participants  │    │ • Audio Buffer   │    │ • Async API     │
│ • RTMS Events   │    │ • File Creation  │    │ • AI Features   │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌──────────────────┐
                       │  Output Files    │
                       │                  │
                       │ • transcript.json│
                       │ • transcript.txt │
                       │ • audio.wav      │
                       └──────────────────┘
```

## 📁 Output Files

The service generates several output files after each meeting:

### Real-time Output
- **Console logs**: Live transcription during the meeting
- **Real-time speaker identification**
- **Progress indicators**

### Post-Meeting Files
- **`transcript_[meeting_id].json`**: Complete transcript with metadata
- **`transcript_[meeting_id].txt`**: Plain text transcript
- **`recording_[meeting_id].wav`**: Audio recording (temporary)

### Sample Transcript JSON
```json
{
  "id": "transcript_12345",
  "text": "Full transcript text...",
  "words": [...],
  "utterances": [
    {
      "speaker": "A",
      "text": "Hello, how are you today?",
      "start": 1000,
      "end": 3000
    }
  ],
  "summary": "• Meeting discussed quarterly results\n• Action items assigned",
  "sentiment_analysis_results": [...],
  "entities": [...]
}
```

## 🛠️ Development

### Project Structure
```
├── index.js              # Main application
├── package.json          # Dependencies
├── .env.example          # Environment template
├── .env                  # Your configuration (create this)
└── README.md            # This file
```

### Key Dependencies
- **express**: Web server for webhooks
- **ws**: WebSocket client for real-time connections
- **node-fetch**: HTTP client for API calls
- **dotenv**: Environment variable management

### Debug Mode
Add debug logging by setting environment variables:
```env
DEBUG_MODE=true
LOG_LEVEL=debug
```

Or add `?debug=1` to enable verbose console output.

## 🔧 API Endpoints

### Webhook Endpoint
- **POST** `/webhook` - Receives Zoom RTMS events

### Status Endpoint  
- **GET** `/` - Basic service information
- Returns service status and configuration

## 🔍 Troubleshooting

### Common Issues

#### "ENOENT: no such file or directory"
- **Cause**: File cleanup race condition
- **Solution**: Restart service, files are cleaned up automatically

#### "Only one of the following models can be enabled"
- **Cause**: Conflicting AssemblyAI features in `ASYNC_CONFIG`
- **Solution**: Disable either `auto_chapters` or `summarization`

#### "WebSocket connection failed"
- **Cause**: Invalid API keys or network issues
- **Solution**: Verify API keys and network connectivity

#### "No audio data received"
- **Cause**: RTMS not properly configured in Zoom
- **Solution**: Check Zoom app RTMS settings and webhook URL

### Debug Information

Enable detailed logging:
```bash
DEBUG_MODE=true npm start
```

Check service status:
```bash
curl http://localhost:8080
```

### Log Examples

**Successful startup:**
```
🎧 Zoom RTMS to AssemblyAI Transcription Service
📋 Configuration:
   Real-time: ✅ (mixed)
   Audio: mono @ 16000Hz
   Async: ✅
CLIENT_ID: YourClientID
🚀 Server running on port 8080
```

**During meeting:**
```
🎤 Meeting started: abc123
🔗 Connecting to AssemblyAI streaming
✅ Zoom signaling connected
🚀 Started audio streaming
📝 [abc123] FINAL: Hello, how are you today?
```

## 📊 Performance

### Resource Usage
- **Memory**: ~50-100MB per active meeting
- **CPU**: Low during standby, moderate during active transcription
- **Network**: ~50KB/s per audio stream
- **Storage**: ~1MB per minute of audio

### Scaling Considerations
- **Concurrent meetings**: Tested up to 10 simultaneous meetings
- **Audio quality**: Higher sample rates increase resource usage
- **Individual mode**: ~2x resource usage vs mixed mode
- **Async processing**: Minimal impact on real-time performance

## 🔒 Security

### Data Privacy
- **Audio streams**: Processed in real-time, not stored permanently
- **Temporary files**: Automatically cleaned up after processing
- **API keys**: Stored securely in environment variables
- **Webhook validation**: HMAC signature verification

### Production Deployment
- Use HTTPS for webhook endpoints
- Store sensitive credentials in secure secret management
- Enable access logging and monitoring
- Consider network security and firewall rules

## 📄 License

MIT License - see LICENSE file for details.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📞 Support

For issues and questions:
- Check the troubleshooting section above
- Review [AssemblyAI Documentation](https://www.assemblyai.com/docs/)
- Check [Zoom RTMS Documentation](https://developers.zoom.us/docs/api/real-time-media-streaming/)
- Open an issue in this repository

---

**Built with ❤️ using Zoom RTMS and AssemblyAI**