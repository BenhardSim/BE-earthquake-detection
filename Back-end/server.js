const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const admin = require('firebase-admin');

const serviceAccount = require('../credentials.json'); // Replace with your service account credentials
const app = express();
const server = http.createServer(app);
const io = socketIo(server);
const cors = require('cors');

admin.initializeApp(
  {
    credential: admin.credential.cert(serviceAccount),
    databaseURL: 'https://eews-pipeline-default-rtdb.asia-southeast1.firebasedatabase.app/', // Replace with your Firebase project URL
  }
);


// Create a reference to your Firebase Realtime Database
const db = admin.database();
app.use(cors());
// Handle incoming socket connections
io.on('connection', (socket) => {
  console.log('A client connected.');

  // Listen for changes in your Firebase database and emit updates to connected clients
  const ref = db.ref('/prediction');
  ref.on('value', (snapshot) => {
    const data = snapshot.val();
    console.log(data)
    socket.emit('data-update', data);
  });

  // Handle socket disconnect
  socket.on('disconnect', () => {
    console.log('A client disconnected.');
    ref.off(); // Stop listening to database changes for this client
  });
});

app.get('/', (req, res) => {
  res.sendFile(__dirname + '/public/index.html'); // Adjust the path as needed
});

server.listen(3000, () => {
  console.log('Server is running on port 3000');
});