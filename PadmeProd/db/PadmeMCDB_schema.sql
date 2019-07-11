-- MySQL Script generated by MySQL Workbench
-- 12/18/18 13:38:53
-- Model: New Model    Version: 1.0
-- MySQL Workbench Forward Engineering

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL,ALLOW_INVALID_DATES';

-- -----------------------------------------------------
-- Schema PadmeMCDB
-- -----------------------------------------------------
-- Database to hold the MonteCarlo production information of the Padme experiment
DROP SCHEMA IF EXISTS `PadmeMCDB` ;

-- -----------------------------------------------------
-- Schema PadmeMCDB
--
-- Database to hold the MonteCarlo production information of the Padme experiment
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `PadmeMCDB` DEFAULT CHARACTER SET utf8 ;
USE `PadmeMCDB` ;

-- -----------------------------------------------------
-- Table `PadmeMCDB`.`production`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `PadmeMCDB`.`production` ;

CREATE TABLE IF NOT EXISTS `PadmeMCDB`.`production` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'Id of production. Positive number.',
  `name` VARCHAR(250) NOT NULL COMMENT 'Unique name to identify production. Should contain some hint to production goal. Free format string. Max length 250 char due to MySQL limitations.',
  `prod_ce` VARCHAR(1024) NULL COMMENT 'URI of the CE where jobs will be submitted.',
  `prod_dir` VARCHAR(1024) NULL COMMENT 'Directory assigned to this production. Contains one subdir per job. Path is relative to production manager directory. Usually set to \"prod/<name>\"',
  `storage_dir` VARCHAR(1024) NULL COMMENT 'Directory path (within PADME storage system) where produced files will be stored. Each production must have its own directory assigned, possibly with dir name = prod name.',
  `srm_uri` VARCHAR(1024) NULL,
  `proxy_file` VARCHAR(1024) NULL COMMENT 'Path to proxy file to use for this production',
  `time_start` DATETIME NULL COMMENT 'Time when the production was created (UTC)',
  `time_end` DATETIME NULL COMMENT 'Time when the production ended (UTC)',
  `n_jobs` INT UNSIGNED NULL COMMENT 'Total number of jobs submitted',
  `n_jobs_ok` INT UNSIGNED NULL COMMENT 'Total number of jobs which completed without errors',
  `n_events` BIGINT UNSIGNED NULL COMMENT 'Total number of events generated by this production',
  PRIMARY KEY (`id`),
  UNIQUE INDEX `name_UNIQUE` (`name` ASC))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `PadmeMCDB`.`job`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `PadmeMCDB`.`job` ;

CREATE TABLE IF NOT EXISTS `PadmeMCDB`.`job` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'Id of job (internal to DB).',
  `production_id` INT UNSIGNED NOT NULL COMMENT 'Production of which this job is part',
  `name` VARCHAR(250) NOT NULL COMMENT 'Name of the job. Usually jobNNNN. Max length set to 200 char due to MySQL UNIQUE key limit.',
  `configuration` TEXT NULL,
  `input_list` TEXT NULL,
  `job_dir` VARCHAR(1024) NULL COMMENT 'Directory used to store job files (script,jdl,log,...). Path is relative to top production manager directory. Usually set to \"prod/<prod_name>/<job_name>\".',
  `random` VARCHAR(1024) NULL COMMENT 'Random settings for this job.',
  `status` INT NULL COMMENT 'Job status\n0 = created but not yet submitted\n1 = REGISTERED, IDLE, PENDING\n2 = RUNNING, REALLY-RUNNING\n3 = DONE-OK\n4 = DONE-FAILED\n5 = DONE-OK with errors in finalization\n6 = DONE-FAILED with errors in finalization',
  `ce_job_id` VARCHAR(1024) NULL COMMENT 'Id assigned to the job by the CE.',
  `worker_node` VARCHAR(1024) NULL COMMENT 'Worker node hostname where the job run.',
  `time_submit` DATETIME NULL COMMENT 'Time when job was submitted (UTC)',
  `time_job_start` DATETIME NULL COMMENT 'Time when job started (UTC)',
  `time_job_end` DATETIME NULL COMMENT 'Time when job ended (UTC)',
  `time_run_start` DATETIME NULL COMMENT 'Time when program actually started (UTC).',
  `time_run_end` DATETIME NULL COMMENT 'Time when program actually ended (UTC).',
  `n_files` INT UNSIGNED NULL COMMENT 'Number of files produced by the job.',
  `n_events` BIGINT UNSIGNED NULL COMMENT 'Total number of events produced by this job.',
  PRIMARY KEY (`id`),
  INDEX `fk_job_production_idx` (`production_id` ASC),
  UNIQUE INDEX `prodid_name_UNIQUE` (`production_id` ASC, `name` ASC),
  CONSTRAINT `fk_job_production`
    FOREIGN KEY (`production_id`)
    REFERENCES `PadmeMCDB`.`production` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `PadmeMCDB`.`file`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `PadmeMCDB`.`file` ;

CREATE TABLE IF NOT EXISTS `PadmeMCDB`.`file` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'Id of the file (internal to DB).',
  `job_id` INT UNSIGNED NOT NULL COMMENT 'Id of the job which produced this file.',
  `name` VARCHAR(250) NOT NULL COMMENT 'Unique name of the file (N.B. filename only, not full path). Max length 250 chars due to MySQL limitations.',
  `type` VARCHAR(1024) NULL COMMENT 'File type (no formal codification yet)',
  `seq_n` INT UNSIGNED NULL COMMENT 'Sequential number of file within job.',
  `time_open` DATETIME NULL COMMENT 'Time when the file was opened (UTC).',
  `time_close` DATETIME NULL COMMENT 'Time when the file was closed (UTC).',
  `n_events` BIGINT UNSIGNED NULL COMMENT 'Number of events in file',
  `size` BIGINT UNSIGNED NULL COMMENT 'Size of file in bytes',
  `adler32` CHAR(8) NULL COMMENT 'ADLER32 checksum for file',
  PRIMARY KEY (`id`),
  UNIQUE INDEX `name_UNIQUE` (`name` ASC),
  INDEX `fk_file_job1_idx` (`job_id` ASC),
  CONSTRAINT `fk_file_job1`
    FOREIGN KEY (`job_id`)
    REFERENCES `PadmeMCDB`.`job` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `PadmeMCDB`.`mc_prod`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `PadmeMCDB`.`mc_prod` ;

CREATE TABLE IF NOT EXISTS `PadmeMCDB`.`mc_prod` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'Id of production. Positive number.',
  `production_id` INT UNSIGNED NOT NULL COMMENT 'Pointer to production',
  `description` TEXT NULL COMMENT 'Free field to hold a human readable description of the production',
  `user_req` VARCHAR(1024) NULL COMMENT 'User(s) who requested the production. Free format field',
  `n_events_req` BIGINT UNSIGNED NULL COMMENT 'Total number of events requested by the user',
  `mc_version` VARCHAR(1024) NULL COMMENT 'Version of the PadmeMC program used in this production.',
  PRIMARY KEY (`id`),
  INDEX `fk_mc_prod_production1_idx` (`production_id` ASC),
  CONSTRAINT `fk_mc_prod_production1`
    FOREIGN KEY (`production_id`)
    REFERENCES `PadmeMCDB`.`production` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `PadmeMCDB`.`reco_prod`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `PadmeMCDB`.`reco_prod` ;

CREATE TABLE IF NOT EXISTS `PadmeMCDB`.`reco_prod` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'Id of production. Positive number.',
  `production_id` INT UNSIGNED NOT NULL COMMENT 'Pointer to production table',
  `description` TEXT NULL COMMENT 'Free field to hold a human readable description of the production',
  `run` VARCHAR(1024) NULL COMMENT 'DAQ run to be reconstructed (1 production = 1 run)',
  `reco_version` VARCHAR(1024) NULL COMMENT 'Version of the PadmeReco program used in this production.',
  PRIMARY KEY (`id`),
  INDEX `fk_reco_prod_production1_idx` (`production_id` ASC),
  CONSTRAINT `fk_reco_prod_production1`
    FOREIGN KEY (`production_id`)
    REFERENCES `PadmeMCDB`.`production` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
