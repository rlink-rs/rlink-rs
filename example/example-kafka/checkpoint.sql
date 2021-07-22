CREATE TABLE `rlink_ck` (
  `application_name` varchar(100) DEFAULT NULL,
  `application_id` varchar(100) DEFAULT NULL,
  `job_id` int DEFAULT NULL,
  `task_number` int DEFAULT NULL,
  `num_tasks` int DEFAULT NULL,
  `operator_id` int DEFAULT NULL,
  `checkpoint_id` bigint DEFAULT NULL,
  `completed_checkpoint_id` bigint DEFAULT NULL,
  `handle` text,
  `create_time` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci 