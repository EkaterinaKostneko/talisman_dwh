-- marts.mart_sprav definition

-- Drop table

-- DROP TABLE marts.mart_sprav;

CREATE TABLE marts.mart_sprav (
	"ID_Хранение" varchar NULL,
	"ID_Контрагенты" varchar NULL,
	"ID_Подразделение" varchar NULL,
	"МестоХран" varchar NULL,
	"Код_Контрагенты" varchar NULL,
	"Аптека" text NULL,
	"Город" text NULL,
	"Филиал" text NULL,
	"Фирма" varchar NULL,
	"Формат" varchar NULL,
	"Круглосуточные" int4 NULL,
	"Широта" float8 NULL,
	"Долгота" float8 NULL,
	"ОбщаяПлощадь" float4 NULL,
	"ТорговаяПлощадь" float4 NULL,
	"КоличествоКасс" int4 NULL,
	"ДатаКоличУчета" timestamp NULL,
	"ТипЦен" varchar NULL,
	"ЦеноваяКатегория" int4 NULL,
	"ГруппаВыкладки" int4 NULL,
	"ДниЗапаса" int4 NULL,
	"ДниЗаказа" int4 NULL,
	"КатегорияАптеки" int4 NULL,
	"КатегорияБонус" int4 NULL,
	"КатегорияМарк" int4 NULL,
	"Аналитик" varchar NULL,
	"УРС" varchar NULL,
	"ГруппаАптекПоТО" varchar NULL
);