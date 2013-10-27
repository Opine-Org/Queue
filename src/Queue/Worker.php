<?php
/**
 * virtuecenter\queue
 *
 * Copyright (c)2013 Ryan Mahoney, https://github.com/virtuecenter <ryan@virtuecenter.com>
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
namespace Queue;

class Worker {
	private $queueGateway;
	private $topic;
	private $root;

	public function __construct ($queueGateway, $topic, $root) {
		$this->queueGateway = $queueGateway;
		$this->topic = $topic;
		$this->root = $root;
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
                $context = $this->queueGateway->watch($queueName)->ignore('default')->reserve();
                $queue->delete($job);
                $context = json_decode($job->getData());
                if (!isset($context['_topic'])) {
                	$this->error('No topic: ' . json_encode($context));
                	continue;
                }
                $topic = $context['_topic'];
                unset($context['_topic']);
                $topic->publish($topic, $context);
                $memory = memory_get_usage();
                if ($memory > 3000000) {
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