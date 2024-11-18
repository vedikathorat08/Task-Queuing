const express = require('express');
const fs = require('fs');
const app = express();

app.use(express.json());

// In-memory data structures
const rateLimits = new Map(); // Tracks rate limits for each user
const taskQueues = new Map(); // Task queues for each user

// Function to log tasks to file
function logTask(user_id) {
    const logEntry = `${user_id}-task completed at-${Date.now()}\n`;
    fs.appendFileSync('task.log', logEntry);
    console.log(logEntry.trim());
}

// Function to process tasks for a specific user
function processTask(user_id) {
    if (!taskQueues.has(user_id) || taskQueues.get(user_id).length === 0) return;

    // Get the next task from the user's queue
    const task = taskQueues.get(user_id).shift();
    logTask(task.user_id);

    // Schedule next task after 1 second
    setTimeout(() => processTask(user_id), 1000);
}

// Endpoint to handle tasks
app.post('/api/v1/task', (req, res) => {
    const { user_id } = req.body;
    if (!user_id) return res.status(400).json({ error: 'user_id is required' });

    const now = Date.now();
    const userRate = rateLimits.get(user_id) || { lastSecond: 0, taskCount: 0, minuteStart: now };

    // Rate limiting logic
    if (userRate.lastSecond + 1000 > now && userRate.taskCount >= 1) {
        // Exceeded 1 task/sec, queue the task
        if (!taskQueues.has(user_id)) taskQueues.set(user_id, []);
        taskQueues.get(user_id).push({ user_id });
        return res.status(202).json({ message: 'Task queued due to rate limit' });
    }

    if (now - userRate.minuteStart < 60000 && userRate.taskCount >= 20) {
        // Exceeded 20 tasks/min, queue the task
        if (!taskQueues.has(user_id)) taskQueues.set(user_id, []);
        taskQueues.get(user_id).push({ user_id });
        return res.status(202).json({ message: 'Task queued due to rate limit' });
    }

    // Reset rate limits if a new minute starts
    if (now - userRate.minuteStart >= 60000) {
        userRate.minuteStart = now;
        userRate.taskCount = 0;
    }

    // Update rate limit and process task
    userRate.lastSecond = now;
    userRate.taskCount += 1;
    rateLimits.set(user_id, userRate);

    // Log the task and schedule next tasks if queued
    logTask(user_id);
    if (!taskQueues.has(user_id)) taskQueues.set(user_id, []);
    setTimeout(() => processTask(user_id), 1000);

    res.status(200).json({ message: 'Task processed immediately' });
});

// Start the server
const PORT = 3000;
app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});
