create table prices (
   itemid int(16) unsigned not null,
   itemname varchar(20) not null,
   price decimal(9,6) signed not null,
   sold int(16) unsigned not null DEFAULT 0,
   primary key (itemid)
); 

create table sales (
   saleid int(16) unsigned not null,
   itemid int(16) unsigned not null,
   saledate date not null,
   quantity int(16) unsigned not null,
   buyername varchar(60) null,
   primary key (saleid)
); 

alter table sales (
   add constraint fk_sales_itemid_prices_itemid foreign key (itemid) references prices (itemid)
);

alter table prices (
   ADD CONSTRAINT chk_prices_sold CHECK (sold IN (0, 1))
);