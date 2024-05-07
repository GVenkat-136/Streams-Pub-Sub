const Redis = require("ioredis");
const express = require('express')
const app = express()

app.use(express.json())
// Redis connection details (replace with your server's IP/hostname and port)
const redisClient = new Redis();


redisClient.on('ready', () => {
    console.log('redis is connected');
});

redisClient.on('error', (err) => {
    console.log('redis is disconnected: ', err);
});

redisClient.on("end", function () {
    console.log("Disconnected from Redis");
});


app.post("/publish", async (req, res) => {
    const { channel, message } = req.body;
    let data;
    for(let i = 0 ; i  <=2 ; i++){
        message.Message = `${message.Message} ${i}`
        await redisClient.xadd('Mychat', "*", "message", JSON.stringify(message));
    }
    await redisClient.xgroup("CREATE", 'Mychat', 'group1', "$", "MKSTREAM");
    data = await redisClient.publish(channel,'Publish');
    res.send(`Message published to channel "${data}"`);
});


app.listen(4003, () => console.log(`Server Running 4003`))

