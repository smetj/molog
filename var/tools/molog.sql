-- phpMyAdmin SQL Dump
-- version 3.3.10deb1
-- http://www.phpmyadmin.net
--
-- Host: localhost
-- Generation Time: May 15, 2011 at 03:37 PM
-- Server version: 5.1.54
-- PHP Version: 5.3.5-1ubuntu7.2

SET SQL_MODE="NO_AUTO_VALUE_ON_ZERO";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;

--
-- Database: `molog`
--

-- --------------------------------------------------------

--
-- Table structure for table `priority`
--

CREATE TABLE IF NOT EXISTS `priority` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `state` varchar(15) NOT NULL,
  `priority` int(1) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM  DEFAULT CHARSET=latin1 AUTO_INCREMENT=7 ;

--
-- Dumping data for table `priority`
--

INSERT INTO `priority` (`id`, `state`, `priority`) VALUES
(1, 'warning', 4),
(2, 'warning', 7),
(3, 'critical', 0),
(4, 'critical', 1),
(5, 'critical', 2),
(6, 'critical', 3);

-- --------------------------------------------------------

--
-- Table structure for table `regexes`
--

CREATE TABLE IF NOT EXISTS `regexes` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `hostname` varchar(256) NOT NULL DEFAULT '.global',
  `regex` varchar(256) DEFAULT '$.',
  `warning` tinyint(1) NOT NULL DEFAULT '1',
  `critical` tinyint(1) NOT NULL DEFAULT '1',
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 AUTO_INCREMENT=1 ;

--
-- Dumping data for table `regexes`
--


-- --------------------------------------------------------

--
-- Table structure for table `results`
--

CREATE TABLE IF NOT EXISTS `results` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `type` varchar(15) NOT NULL,
  `rsyslog_id` int(11) NOT NULL,
  `deleted` int(1) NOT NULL DEFAULT '0',
  `old` int(1) NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 AUTO_INCREMENT=1 ;

--
-- Dumping data for table `results`
--

