CREATE TABLE `dk_deck` (
   dk_id INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
   dk_name VARCHAR(150) NOT NULL,
   dk_cards VARCHAR(2000) NOT NULL,

   PRIMARY KEY (dk_id)
) ENGINE=INNODB DEFAULT CHARSET=utf8 ROW_FORMAT=COMPRESSED;


