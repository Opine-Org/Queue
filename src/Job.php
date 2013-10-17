<?php
abstract class Job {
	protected static function log ($msg) {
		file_put_contents(__DIR__ . '/queue_log.txt', $msg . "\n", FILE_APPEND);
	}

    private static function validateObject (&$obj) {
        if (!method_exists($obj, '__wakeup')) {
            throw new \Exception ('Object added to generic without __wakeup method defined.');
        }
    }
	
	public static function add ($obj) {
        self::validateObject($obj);
        $queue = new Pheanstalk_Pheanstalk('127.0.0.1');
        $queue->
			useTube('soulcycle')->
			put(serialize($obj));
	}

	public static function worker () {
        require_once(__DIR__ . '/Pheanstalk/pheanstalk_init.php');
		require_once(__DIR__ . '/../job/ReservationRequestJob.php');
        $queue = new Pheanstalk_Pheanstalk('127.0.0.1');
        try {
            while(1) {
                if ($queue->getConnection()->isServiceListening() != true) {
                    unset($queue);
                    $queue = new Pheanstalk_Pheanstalk('127.0.0.1');
                }
                $job = $queue->watch('soulcycle')->ignore('default')->reserve();
                $queue->delete($job);
                $job = unserialize($job->getData());
                $memory = memory_get_usage();
                if ($memory > 3000000) {
                    file_put_contents(__DIR__ . '/worker.log', 'Memory exceeded.' . "\n", FILE_APPEND);
                    self::log('exiting run due to memory limit');
                    exit;
                }
                usleep(10);
            }
        } catch (Exception $e) {
            file_put_contents(__DIR__ . '/worker.log', $e->getMessage() . "\n", FILE_APPEND);
        }
	}
}