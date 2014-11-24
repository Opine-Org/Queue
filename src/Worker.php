<?php
/**
 * Opine\Worker
 *
 * Copyright (c)2013, 2014 Ryan Mahoney, https://github.com/Opine-Org <ryan@virtuecenter.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
namespace Opine\Queue;

use ArrayObject;

class Worker {
    private $queueGateway;
    private $topic;
    private $root;

    public function __construct ($root, $queueGateway, $topic) {
        $this->queueGateway = $queueGateway;
        $this->topic = $topic;
        $this->root = $root;
        $pidFile = $this->root . '/../worker.pid';
        if (file_exists($pidFile)) {
            $pid = trim(file_get_contents($pidFile));
            @shell_exec('kill -s 9 ' . $pid);
        }
        file_put_contents($pidFile, getmypid());
    }

    public function work ($queueName=false) {
        if ($queueName === false) {
            $queueName = $this->root;
        }
        try {
            while(true) {
                if ($this->queueGateway->getConnection()->isServiceListening() != true) {
                    unset($this->queueGateway);
                    $this->error('Queue is not listening.');
                    exit();
                }
                $job = $this->queueGateway->watch($queueName)->ignore('default')->reserve();
                if (!is_object($job)) {
                    sleep(3);
                    continue;
                }
                $this->queueGateway->delete($job);
                $context = (array)json_decode($job->getData(), true);
                if (!isset($context['_topic'])) {
                    $this->error('No topic: ' . json_encode($context));
                    continue;
                }
                $topic = $context['_topic'];
                unset($context['_topic']);
                $result = $this->topic->publish($topic, new ArrayObject($context));
                $memory = memory_get_usage();
                if ($memory > 10000000) {
                    $this->error('Worker exiting due to memory limit');
                    exit;
                }
                usleep(10);
            }
        } catch (Exception $e) {
            $this->error('Job exception:' . $e->getMessage());
        }
    }

    private function error ($message) {
        file_put_contents('php://stderr', $message . "\n");
    }
}