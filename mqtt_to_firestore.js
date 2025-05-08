const express = require('express');
const app = express();
const port = process.env.PORT || 3000;

// Tạo một HTTP endpoint đơn giản để Render nhận biết dịch vụ
app.get('/', (req, res) => {
  res.send('MQTT to Firestore is running ✅');
});

// Khởi động HTTP server
app.listen(port, () => {
  console.log(`🌐 Web server is running on port ${port}`);
});

// ======== PHẦN GỐC: MQTT to Firestore =========
const mqtt = require('mqtt');
const admin = require('firebase-admin');
const fs = require('fs');

// Giải mã chuỗi Base64 từ biến môi trường và ghi vào tệp tạm thời
const serviceAccountBase64 = process.env.GOOGLE_APPLICATION_CREDENTIALS;
const serviceAccountBuffer = Buffer.from(serviceAccountBase64, 'base64');
const tempFilePath = '/tmp/service-account.json';
fs.writeFileSync(tempFilePath, serviceAccountBuffer);

// Khởi tạo Firebase Admin
admin.initializeApp({
  credential: admin.credential.cert(tempFilePath),
});
const db = admin.firestore();

// MQTT broker config
const mqttOptions = {
  username: 'xinruhd',
  password: 'xr621Hd168',
};
const client = mqtt.connect('mqtt://h.ceosz.com:1883', mqttOptions);

client.on('connect', async () => {
  console.log('✅ Connected to MQTT broker');

  try {
    const snapshot = await db.collection('Models').get();
    snapshot.forEach((doc) => {
      const modelId = doc.id;
      const topic = `up/a1JXGupSRBK/${modelId}`;
      client.subscribe(topic, (err) => {
        if (err) {
          console.error(`❌ Không subscribe được topic ${topic}:`, err);
        } else {
          console.log(`📡 Đã subscribe topic: ${topic}`);
        }
      });
    });
  } catch (err) {
    console.error('❌ Lỗi khi lấy model từ Firestore:', err);
  }
});

client.on('message', async (topic, message) => {
  try {
    const topicParts = topic.split('/');
    const modelId = topicParts[2];
    const data = JSON.parse(message.toString());

    if (!data.id || data.id.toString() !== '60006') return;

    let deviceId = '';
    if (data.params && data.params.DetailMsg) {
      let detailObj = data.params.DetailMsg;
      if (typeof detailObj === 'string') {
        try {
          detailObj = JSON.parse(detailObj);
        } catch (_) {}
      }
      if (detailObj && detailObj.DeviceID) {
        deviceId = detailObj.DeviceID.toString();
      }
    }

    if (!deviceId) {
      console.warn('⚠️ Không tìm thấy deviceId trong payload:', data);
      return;
    }

    const modelMetaDoc = await db.collection('Models').doc(modelId).get();
    if (!modelMetaDoc.exists || !modelMetaDoc.data().userId) {
      console.error('❌ Không tìm thấy userId cho modelId:', modelId);
      return;
    }
    const foundUserId = modelMetaDoc.data().userId;

    const notification = {
      createdAt: new Date().toISOString(),
      deviceId,
      id: data.id.toString(),
      method: data.method ? data.method.toString() : '',
      modelId,
      detailMsg:
        typeof data.params.DetailMsg === 'string'
          ? data.params.DetailMsg
          : JSON.stringify(data.params.DetailMsg),
      version: data.version ? data.version.toString() : '',
    };

    const deviceRef = db
      .collection('users')
      .doc(foundUserId)
      .collection('models')
      .doc(modelId)
      .collection('devices')
      .doc(deviceId);

    const doc = await deviceRef.get();
    let history = [];
    if (doc.exists && doc.data().notificationHistory) {
      history = doc.data().notificationHistory;
    }

    history.unshift(notification);
    const maxHistory = 50;
    history = history.slice(0, maxHistory);

    await deviceRef.set(
      {
        notificationHistory: history,
        updatedAt: new Date().toISOString(),
      },
      { merge: true }
    );

    console.log(`✅ Notification saved for model ${modelId}, device ${deviceId}`);
  } catch (err) {
    console.error('❌ Error processing MQTT message:', err);
  }
});
