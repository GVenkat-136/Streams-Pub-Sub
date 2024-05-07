// Import required modules
const express = require("express");
const Redis = require('ioredis')
const app = express();
const port = 3001;
let consumerName = null;

const redisClient =  new Redis();

const redisSub =  new Redis();



redisSub.on('ready', () => console.log('redis connected'))

redisSub.on('message', async (channel, message) => {
    await processMessages('Mychat', "group1");
});


async function processMessages(streamName, GroupName) {
try {
    consumerName = await consumerInfo(streamName, GroupName);
    console.log(GroupName,streamName,consumerName)
    const readNewGroupMessgae = await redisClient.xreadgroup("GROUP", GroupName, consumerName, "COUNT", 3, "BLOCK", "1000", "STREAMS", streamName, '>');
    if (readNewGroupMessgae && readNewGroupMessgae[0] && readNewGroupMessgae[0][1] && readNewGroupMessgae[0][1].length > 0) {
        const messagesData = readNewGroupMessgae[0][1];
        for (const [messageId, messageData] of messagesData) {
            let parsedData = messageData[1];
            console.log(parsedData)
            await redisClient.xack(streamName, GroupName, messageId);
        }
    }
}catch (error) {
    console.log(error)
}
}


async function createConsumer(streamName, groupName ) {
    try {
        const groupDetails = await redisClient.xinfo("GROUPS", streamName);
        for (let i = 0; i < 100; i++) {
            consumerName = `Consumer:${groupDetails.length + i + 1}`;
            await redisClient.xgroup("CREATECONSUMER", streamName, groupName, consumerName);
        }
    } catch (e) {
        console.log(e, "ERROR");
    }
}

async function consumerInfo(streamName,groupName) {
    try {
        const consumerInfo= await redisClient.xinfo("CONSUMERS", streamName, groupName);
        consumerName = await findAvailableConsumerInfo(consumerInfo);
        if (!consumerName) {
            await createConsumer(streamName, groupName);
        }
        consumerName = await findAvailableConsumerInfo(consumerInfo);
        return consumerName;
    } catch (e) {
        console.log(e, "ERROR");
    }
}

async function findAvailableConsumerInfo(consumerInfo) {
    for (const consumer of consumerInfo) {
        const [_, consumerName, status, pendingCount, __, ___] = consumer;
        if (status === 'pending' && pendingCount <= 0 && consumerName.length > 0) {
            return consumerName;
        }
    }
}

app.listen(port, async () => {
    await redisSub.subscribe('my_channel');
    console.log(`Server is running on http://localhost:${port}`);
});
