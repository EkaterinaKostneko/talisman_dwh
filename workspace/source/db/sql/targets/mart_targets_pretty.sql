DROP  VIEW IF EXISTS marts.mart_targets_pretty;

CREATE OR REPLACE VIEW marts.mart_targets_pretty
AS
SELECT
    tdate     		"Дата",
    idstore     	"ID аптеки",
    pharmacy        "ID2 аптеки",
    address         "НазваниеАптеки",
    averagebill		"СреднийЧекПлан",
    revenue	    	"ВыручкаПлан",
    points			"БаллыПлан",
    markup          "НаценкаФактическая",
    markuplast      "ПроцентНаценки",
    profit          "ВаловаяПрибыльПлан",
    traffic         "ПроходимостьПлан"
FROM marts.mart_targets

