CREATE TABLE `cdi_card_decks_index` (
   cdi_card_id MEDIUMINT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
   cdi_deck_ids LONGTEXT NOT NULL,

   PRIMARY KEY (cdi_card_id)
) ENGINE=INNODB DEFAULT CHARSET=utf8 ROW_FORMAT=COMPRESSED;


UPDATE `cdi_card_decks_index`
SET `cdi_sideboard_appearances` = CONCAT(IF(`cdi_sideboard_appearances` IS NULL, '', `cdi_sideboard_appearances`), 'hello') WHERE `cdi_card_id`=1