# RTMS Financial Consultation Intelligence - Node.js

This project demonstrates how to build an AI-powered financial consultation assistant using Zoom's RTMS (Real-Time Media Streaming) with AssemblyAI's Universal Streaming v3 and Anthropic's Claude for real-time conversation analysis.

## 🎯 What This System Does

This intelligent assistant monitors financial consultation conversations in real-time and provides:

- **📝 Live Transcription**: Real-time speech-to-text using AssemblyAI's latest Universal Streaming v3 model with speaker identification
- **💎 FAINT Qualification**: Tracks Funds, Authority, Interest, Need, and Timing throughout the conversation
- **🧠 Smart Analysis**: Uses Claude AI to extract key insights and identify opportunities
- **💡 Advisor Coaching**: Real-time suggestions and reminders for financial advisors
- **⚠️ Concern Detection**: Identifies client worries and suggests addressing strategies
- **❓ Strategic Questions**: AI-generated questions to gather more valuable information
- **📊 Live Web Dashboard**: Beautiful web interface with real-time updates and dual-tab view
- **👥 Speaker Management**: Assign roles (Consultant/Client) to different speakers in the meeting

## 🌐 Web Interface Features

### Dual-Tab Dashboard
- **📊 Financial Intelligence Tab**: Live FAINT analysis, client insights, and advisor recommendations
- **📝 Live Transcript Tab**: Real-time conversation transcript with speaker labels and timestamps
- **👥 Speaker Controls**: Assign and manage speaker roles (Consultant/Client) during the meeting
- **🔄 Configurable Refresh**: User-adjustable update intervals (500ms - 30000ms, default 3000ms)
- **⏸️ Pause/Resume Controls**: Ability to pause updates when interacting with UI components
- **🗂️ Collapsible Sections**: Show latest items by default with "Show All" expand functionality
- **📱 Responsive Design**: Works seamlessly on desktop, tablet, and mobile
- **🐛 Debug Mode**: Add `?debug=1` to URL for detailed logging and troubleshooting

> [!TIP]
> **Important UI Usage Notes:**
> - **Pause refresh when changing speaker labels** to prevent interruption during role assignment
> - **Pause refresh when expanding "Show All" sections** to avoid UI state conflicts
> - **Use the ⏸️ Pause button** next to the update interval control for stable interaction with dropdown menus and expandable sections

### Professional UI Features
- **Modern Design**: Gradient backgrounds, smooth animations, and card-based layout
- **Color-Coded Insights**: Visual distinction between concerns, questions, and reminders
- **Speaker-Coded Transcripts**: Different colors for Consultant vs Client speech
- **Status Indicators**: Real-time system health monitoring in the header
- **Smart Data Preservation**: FAINT data and client information retained unless new updates are available
- **Latest-First Display**: Show most recent items prominently with collapsible history
- **Empty States**: Helpful messaging when waiting for consultation to begin

### Data Management
- **📝 Running Lists**: Client information, consultation summary, and other data types maintain historical records
- **🔒 Data Preservation**: Existing FAINT qualification data is preserved unless new information is detected
- **📊 Smart Updates**: AI only updates fields with genuinely new or changed information
- **🗂️ Collapsible History**: View latest items by default, expand to see full history when needed

## 🏗️ Setup

### Prerequisites
- Node.js 18+ 
- Zoom account with RTMS enabled
- Zoom App SDK integration (for in-meeting access)
- AssemblyAI API key
- Anthropic API key
- ffmpeg (for audio processing)
- ngrok or similar tunneling service for development

### Zoom App Configuration

#### 1. Create a Zoom App
1. Go to [Zoom Marketplace](https://marketplace.zoom.us/) and create a new app
2. Choose "General App" type
3. Fill in basic app information

#### 2. Configure App Features
Under the **App Features** section:

**Zoom App SDK:**
- Click the **+ Add APIs** button and enable the following options:
  - **APIs**: `shareApp`
  
**Scopes:**
- Ensure that the following scope is selected on the **Scopes** tab:
  - `zoomapp:inmeeting`

#### 3. RTMS Configuration
1. Enable **Real-Time Media Streaming (RTMS)**
2. Configure webhook URL: `https://your-domain.com/webhook`
3. Enable webhook events:
   - `meeting.rtms_started`
   - `meeting.rtms_stopped`
   - `endpoint.url_validation`

#### 4. App URLs
Configure the following URLs in your Zoom App:
- **Home URL**: `https://your-domain.com/`
- **Redirect URL**: `https://your-domain.com/`

### Installation

1. **Clone and install dependencies**:
```bash
git clone https://github.com/zkleb-aai/rtms_assist_demo.git
cd rtms-financial-consultation
npm install
```

2. **Install required packages**:
```bash
npm install 
```

### Configuration

1. **Create environment file**:
```bash
cp .env.example .env
```

2. **Configure your .env file**:
```env
# Zoom RTMS Configuration
ZOOM_SECRET_TOKEN=your_zoom_secret_token
ZM_CLIENT_ID=your_zoom_client_id
ZM_CLIENT_SECRET=your_zoom_client_secret

# AI Services
ASSEMBLYAI_API_KEY=your_assemblyai_api_key_here
ANTHROPIC_API_KEY=your_anthropic_api_key_here

# Development (optional)
NODE_ENV=development
DEBUG_MODE=false
PORT=8080
```

3. **Set up ngrok for development**:
```bash
# Install ngrok globally
npm install -g ngrok

# Start your app
npm start

# In another terminal, expose your app
ngrok http 8080
```

4. **Update Zoom App URLs**:
   - Use your ngrok URL (e.g., `https://abc123.ngrok.io`) for all webhook and app URLs in the Zoom Marketplace configuration

## 🚀 Usage

### Start the System
```bash
npm start
```

The system will start on port 8080 and display:
```
🌐 Financial Consultation Intelligence System running at http://localhost:8080
🔗 Webhook endpoint at http://localhost:8080/webhook
🐛 Debug mode: ENABLED/DISABLED
📋 UI UPDATES: Client info as running list, configurable intervals with pause/resume
```

### Access the Dashboard
- **Direct Access**: Open `http://localhost:8080` in your browser
- **In-Meeting Access**: Open the app from within a Zoom meeting using the Apps panel

### Dashboard Controls

#### Update Interval Configuration
- **Default Interval**: 3000ms (3 seconds)
- **Adjustable Range**: 500ms to 30000ms
- **Settings Persistence**: Interval preferences saved in browser localStorage
- **Real-Time Changes**: New interval applied immediately

#### Pause/Resume Functionality
- **⏸️ Pause Button**: Located next to update interval control
- **When to Use Pause**:
  - Before changing speaker role assignments
  - When expanding "Show All" sections
  - During detailed data review
  - To prevent UI interruptions during interactions
- **Visual Feedback**: Button changes to "▶️ Resume" when paused
- **Status Display**: Pause state shown in all status messages

### Start a Consultation
1. **Join/Start a Zoom meeting**
2. **Open the app** from the Zoom Apps panel (📱 Apps button in meeting toolbar)
3. **Enable RTMS** if prompted
4. The dashboard will automatically switch to "🟢 ACTIVE" status
5. **Pause refresh** using the ⏸️ button before assigning speaker roles
6. **Assign speaker roles** using the Speaker Assignment controls
7. **Resume refresh** to continue monitoring
8. Watch real-time transcription and AI analysis appear in both tabs
9. Monitor financial insights as the conversation progresses

### Speaker Management
- **Auto-Detection**: System automatically detects when new speakers join
- **Role Assignment**: Use dropdown controls to assign "Consultant" or "Client" roles
- **⚠️ Best Practice**: Always pause refresh before changing speaker assignments
- **Real-Time Updates**: Transcript labels update immediately when roles are changed
- **Solo Testing**: First detected speaker is automatically assigned as "Consultant"

### Data Interaction
- **Latest-First View**: Most recent items displayed prominently
- **"Show All" Expansion**: Click expand buttons to see full history
- **⚠️ Best Practice**: Pause refresh before expanding sections to prevent UI conflicts
- **Preserved Data**: FAINT qualification and other data retained unless genuinely updated

## 🎭 System Flow

1. **🎧 Zoom Connection**: Direct WebSocket connection to Zoom RTMS (no SDK required for audio)
2. **👥 Speaker Detection**: Automatic speaker identification using Zoom's user_id system
3. **🎙️ Audio Streaming**: Base64-decoded audio chunks sent to AssemblyAI in 250ms intervals
4. **📝 Real-Time Transcription**: AssemblyAI v3 provides ultra-low latency speech-to-text with speaker labels
5. **🤖 AI Analysis**: Claude 3.5 Sonnet analyzes each transcript segment for financial insights
6. **🔒 Data Preservation**: Smart updates that preserve existing information unless new data is detected
7. **📊 Live Updates**: Web dashboard refreshes at user-configurable intervals (default 3000ms)
8. **💾 Data Persistence**: Conversation logs and final reports saved to `./consultation_logs/`

## 📊 AI Analysis Features

### FAINT Qualification Framework
- **💰 Funds**: Financial capacity and assets identification
- **👤 Authority**: Decision-making power assessment  
- **🎯 Interest**: Engagement and investment appetite
- **🎪 Need**: Financial goals and problems identification
- **⏰ Timing**: Timeline for financial decisions
- **🔒 Data Preservation**: Existing FAINT data retained unless new information is detected

### Smart Coaching
- **Consultation Summary**: Key developments extracted automatically (running list)
- **Client Information**: Personal and financial background compilation (running list)
- **Advisor Reminders**: AI-generated coaching suggestions (running list)
- **Client Concerns**: Worry identification with addressing strategies (running list)
- **Strategic Questions**: Specific questions to gather valuable information (running list)

### Intelligent Data Management
- **Incremental Updates**: Only update fields with new or changed information
- **Historical Preservation**: Maintain full conversation context and data history
- **Smart Field Detection**: AI identifies when to add vs. update vs. preserve data
- **Running Lists**: All data types (except FAINT) maintain chronological records

## 🛠️ Technical Architecture

### Audio Processing Pipeline
- **Direct RTMS Protocol**: WebSocket connection without SDK dependencies
- **Speaker Identification**: Uses Zoom's user_id system for speaker tracking
- **Base64 Audio Decoding**: Handles Zoom's JSON-based audio format
- **Buffered Streaming**: 250ms chunks optimized for AssemblyAI v3
- **Real-Time Processing**: Sub-second latency from speech to insights

### Web Dashboard Architecture
- **Express.js Backend**: RESTful API for dashboard data
- **Configurable Updates**: User-adjustable refresh intervals with pause/resume
- **Responsive Frontend**: Mobile-first design with CSS Grid/Flexbox
- **Security Headers**: Helmet.js with Content Security Policy including HSTS
- **State Management**: Intelligent UI state preservation during interactions
- **Error Handling**: Graceful degradation and connection recovery

### AI Integration
- **Anthropic Claude 3.5**: Tool-based structured data extraction with smart field updates
- **AssemblyAI v3**: Universal Streaming model with turn detection
- **Conversation Context**: Full conversation history with speaker attribution
- **Data Preservation**: Smart updates that preserve existing valuable information
- **Real-Time Processing**: Insights generated as conversation progresses

## 🔧 API Endpoints

### Dashboard APIs
- `GET /` - Main dashboard interface with configurable refresh and pause controls
- `GET /api/status` - System health and status
- `GET /api/dashboard` - Financial intelligence data
- `GET /api/transcript` - Live transcript data
- `GET /api/speakers` - Speaker detection and roles
- `POST /api/speakers/assign` - Assign speaker roles
- `GET /api/debug/speakers` - Debug speaker information
- `GET /api/debug/speakers/detailed` - Detailed speaker analysis
- `GET /api/debug/speakers/export` - Export speaker logs

### Webhook Endpoints
- `POST /webhook` - Zoom RTMS webhook handler

## 📁 Output Files

The system generates structured JSON files:
- `consultation_logs/{meeting_id}.json` - Live conversation tracking with preserved data
- `consultation_logs/{meeting_id}_final_report.json` - Complete analysis with full transcript and speaker mapping

### Sample Output Structure
```json
{
  "meetingId": "meeting_12345",
  "timestamp": "2024-01-15T10:30:00.000Z",
  "speakerMapping": [[0, "Consultant"], [1, "Client"]],
  "conversationHistory": [...],
  "financialData": {
    "summary": [
      "Client has $5M in assets", 
      "Interested in portfolio diversification",
      "Discussed risk tolerance preferences"
    ],
    "faint": {
      "funds": "$5 million in bank deposits and investment accounts",
      "authority": "Primary decision maker for family finances",
      "interest": "High interest in wealth management and risk mitigation",
      "need": "Portfolio diversification and retirement planning",
      "timing": "Looking to implement strategy within 3 months"
    },
    "clientInfo": [
      "Retired executive, age 65, conservative risk profile",
      "Married with two adult children",
      "Primary residence worth $2M, mortgage-free"
    ],
    "advisorReminders": [
      "Follow up on risk tolerance questionnaire",
      "Prepare estate planning discussion materials",
      "Schedule follow-up meeting in 2 weeks"
    ],
    "concerns": [{
      "concern": "Market volatility affecting retirement funds",
      "addressing_strategy": "Discuss diversification strategies and conservative investment options"
    }],
    "strategicQuestions": [{
      "question": "What percentage of your portfolio would you be comfortable having in higher-risk investments?",
      "purpose": "To better understand risk tolerance and investment preferences"
    }]
  },
  "fullTranscript": "Consultant: Good morning, thank you for meeting with me today..."
}
```

## 🔒 Security & Privacy

- **Secure Audio Handling**: Audio processed in real-time, not stored persistently
- **Local Data Storage**: All conversation logs stored locally
- **Environment Variables**: Secure API key management
- **HTTPS Support**: Production-ready with proper SSL termination
- **Security Headers**: Helmet.js with HSTS, CSP, and other OWASP-compliant headers
- **Speaker Privacy**: Speaker identification handled securely with role-based labeling
- **Financial Compliance**: Designed for regulated financial environments
- **Data Preservation**: Smart updates preserve sensitive information appropriately

## 🐛 Debugging & Troubleshooting

### Debug Mode
Add `?debug=1` to your URL to enable:
- **Console Logging**: Detailed speaker detection and assignment logs
- **Debug Panel**: Shows current speaker state and update intervals in the UI
- **API Inspection**: Direct links to debug endpoints
- **Performance Monitoring**: Update frequency and pause state tracking

### Best Practices for UI Interaction
1. **Before changing speaker assignments**: Click ⏸️ Pause to prevent interruption
2. **Before expanding "Show All" sections**: Pause refresh to avoid state conflicts
3. **During detailed data review**: Use pause to prevent screen updates
4. **After UI interactions**: Resume refresh to continue monitoring

### Common Issues

1. **Speaker Assignment Dropdown Closes Unexpectedly**:
   - **Solution**: Pause refresh before changing speaker roles
   - Check that updates are paused in the status bar

2. **"Show All" Sections Collapse Automatically**:
   - **Solution**: Pause refresh before expanding sections
   - Resume after reviewing expanded content

3. **Update Interval Changes Don't Persist**:
   - Settings are saved to localStorage automatically
   - Check browser console for any storage errors

4. **FAINT Data Gets Cleared**:
   - This should no longer happen with the improved data preservation
   - Check that AI is only updating fields with new information

5. **No Audio/Transcription**:
   - Verify RTMS is properly enabled in Zoom
   - Check AssemblyAI API key in environment
   - Monitor server console for connection errors

6. **AI Analysis Not Working**:
   - Verify Anthropic API key is set
   - Check conversation logs for processing errors
   - Ensure transcript data is being received

### Debug Endpoints
- `GET /api/debug/speakers` - Current speaker state
- `GET /api/debug/speakers/detailed` - Comprehensive speaker analysis
- `GET /api/debug/speakers/export` - Download complete logs
- Browser console with debug mode for real-time logs

## 🚀 Production Deployment

### Environment Setup
```bash
# Production environment variables
NODE_ENV=production
DEBUG_MODE=false
PORT=8080
```

### Security Configuration
The application includes production-ready security headers:
- **HSTS**: Strict-Transport-Security with 1-year max-age
- **CSP**: Content Security Policy for XSS prevention
- **Other Headers**: Referrer policy and additional OWASP protections

### Docker Deployment
```dockerfile
FROM node:18-alpine
WORKDIR /app
COPY package*.json ./
RUN npm ci --only=production
COPY . .
EXPOSE 8080
CMD ["npm", "start"]
```

### Health Monitoring
The system provides health endpoints:
- `GET /api/status` - System health and status with configurable update tracking
- Built-in memory usage and uptime monitoring
- Automatic connection recovery and error handling
- Update interval and pause state monitoring

## 📱 Mobile Support

The dashboard is fully responsive and optimized for:
- **Desktop**: Full dual-pane view with rich interactions and configurable refresh controls
- **Tablet**: Adaptive layout with touch-friendly controls and pause functionality
- **Mobile**: Single-column layout with swipe navigation between tabs
- **In-Meeting Mobile**: Optimized for Zoom mobile app integration with simplified controls

## 🎯 Use Cases

- **Financial Advisory Meetings**: Real-time client qualification and coaching with speaker identification
- **Sales Consultations**: Lead qualification and objection handling with role-based analysis
- **Compliance Monitoring**: Automatic conversation logging with speaker attribution and data preservation
- **Training & Development**: Advisor performance insights with conversation analysis
- **Client Relationship Management**: Structured data extraction for CRM integration with running lists
- **Solo Testing**: Development and testing with single-speaker scenarios

## 💡 Advanced Features

### Smart Data Management
- **Intelligent Updates**: Only update fields with genuinely new information
- **Data Preservation**: Existing valuable information is never overwritten unnecessarily
- **Running Lists**: Maintain chronological records of all insights and observations
- **Latest-First Display**: Show most recent and relevant information prominently

### User Experience Enhancements
- **Configurable Refresh**: User-adjustable update intervals (500ms - 30000ms)
- **Pause/Resume Controls**: Seamless interaction without UI conflicts
- **Collapsible Sections**: Manage information density with expand/collapse functionality
- **Smart State Management**: UI state preserved during interactions

### Speaker Intelligence
- **Automatic Detection**: Identifies new speakers joining the conversation
- **Role-Based Analysis**: Different AI insights based on speaker roles
- **Solo Support**: Optimized for single-speaker testing and development
- **Dynamic Assignment**: Change speaker roles during live conversations with pause protection

### Real-Time Processing
- **Sub-Second Latency**: From speech to transcript to AI insights
- **Adaptive Refresh**: User-controlled update frequency
- **Buffered Streaming**: Optimized audio chunks for best transcription quality
- **Connection Recovery**: Automatic reconnection on network issues

This system transforms traditional financial consultations into data-driven, AI-enhanced experiences that help advisors provide better service while ensuring no critical information is missed. The beautiful web interface makes it easy to monitor conversations in real-time and access valuable insights instantly, with intelligent data preservation and user-friendly interaction controls for optimal usability.
