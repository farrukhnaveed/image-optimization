DROP TABLE IF EXISTS `fc_rental_photos_optimized`;
CREATE TABLE `fc_rental_photos_optimized` (
  `id` int NOT NULL AUTO_INCREMENT,
  `product_id` int DEFAULT NULL,
  `product_image_dir` varchar(100) NOT NULL DEFAULT '',
  `product_image` varchar(1000) NOT NULL,
  `original_image_extension` varchar(5) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `imgPriority` int NOT NULL DEFAULT '0',
  `caption` text CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci,
  `resolution_type` enum('small','medium','large') DEFAULT NULL,
  `resolution_width`  int DEFAULT NULL,
  `resolution_height`  int DEFAULT NULL,
  `image_size` float DEFAULT NULL,
  `status` enum('Active','Deleted','Pending') NOT NULL DEFAULT 'Active',
  `queue_id` int DEFAULT NULL,
  `s3_bucket` varchar(64) DEFAULT NULL,
  `imgtitle` varchar(1000) DEFAULT NULL,
  `mproduct_image` varchar(1000) DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_product_id` (`product_id`),
  KEY `IDX_Status` (`status`),
  KEY `idx_resolution_width` (`resolution_width`),
  KEY `idx_resolution_height` (`resolution_height`),
  KEY `idx_image_size` (`image_size`),
  KEY `idx_queue_id` (`queue_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


DROP TABLE IF EXISTS `fc_photos_optimization_queue`;
CREATE TABLE `fc_photos_optimization_queue` (
  `id` int NOT NULL AUTO_INCREMENT,
  `status` enum('pending','pushed','running','completed','rejected','stopped') NOT NULL DEFAULT 'pending',
  `source` varchar(100) NOT NULL,
  `source_id` int NOT NULL,
  `destination` varchar(100) NOT NULL,
  `image` varchar(1000) NOT NULL,
  `is_blur` tinyint NOT NULL DEFAULT '0',
  `should_enhance` tinyint NOT NULL DEFAULT '0',
  `is_enhanced` tinyint NOT NULL DEFAULT '0',
  `is_optimized` tinyint NOT NULL DEFAULT '0',
  `is_reduced` tinyint NOT NULL DEFAULT '0',
  `allow_blur` tinyint NOT NULL DEFAULT '0',
  `allow_optimize` tinyint NOT NULL DEFAULT '0',
  `allow_reduce` tinyint NOT NULL DEFAULT '0',
  `allow_resolution_size` tinyint NOT NULL DEFAULT '0',
  `message` text,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `IDX_status` (`status`),
  KEY `idx_source` (`source`),
  KEY `idx_source_id` (`source_id`),
  KEY `idx_destination` (`destination`),
  KEY `idx_is_blur` (`is_blur`),
  KEY `idx_should_enhance` (`should_enhance`),
  KEY `idx_is_enhanced` (`is_enhanced`),
  KEY `idx_is_optimized` (`is_optimized`),
  KEY `idx_is_reduced` (`is_reduced`),
  KEY `idx_allow_blur` (`allow_blur`),
  KEY `idx_allow_optimize` (`allow_optimize`),
  KEY `idx_allow_reduce` (`allow_reduce`),
  KEY `idx_allow_resolution_size` (`allow_resolution_size`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


DROP TABLE IF EXISTS `fc_photos_optimization_queue_audit_log`;
CREATE TABLE `fc_photos_optimization_queue_audit_log` (
  `id` int NOT NULL AUTO_INCREMENT,
  `queue_id` int NOT NULL,
  `status` enum('pending','pushed','running','completed','rejected','stopped') NOT NULL DEFAULT 'pending',
  `source` varchar(100) NOT NULL,
  `source_id` int NOT NULL,
  `step` varchar(32) NOT NULL,
  `error` tinyint NOT NULL DEFAULT '0',
  `message` varchar(100) NOT NULL,
  `detail` text,
  `queue_state` varchar(1000),
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `IDX_queue_id` (`queue_id`),
  KEY `idx_status` (`status`),
  KEY `idx_source` (`source`),
  KEY `idx_source_id` (`source_id`),
  KEY `idx_step` (`step`),
  KEY `idx_error` (`error`),
  KEY `idx_queue_state` (`queue_state`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;