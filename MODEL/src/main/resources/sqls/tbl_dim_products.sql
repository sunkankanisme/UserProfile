create table if not exists tbl_dim_products
(
    id           int(11) not null auto_increment primary key,
    product_name varchar(255) default null
) auto_increment = 29
  default charset = utf8;

insert into tbl_dim_products value ('1', '4K电视');
insert into tbl_dim_products value ('2', 'Haier/海尔冰箱');
insert into tbl_dim_products value ('3', 'LED电视');
insert into tbl_dim_products value ('4', 'Leader/统帅冰箱');
insert into tbl_dim_products value ('5', '冰吧');
insert into tbl_dim_products value ('6', '冷柜');
insert into tbl_dim_products value ('7', '净水机');
insert into tbl_dim_products value ('8', '前置过滤器');
insert into tbl_dim_products value ('9', '取暖电器');
insert into tbl_dim_products value ('10', '吸尘器/除螨仪');
insert into tbl_dim_products value ('11', '嵌入式厨电');
insert into tbl_dim_products value ('12', '微波炉');
insert into tbl_dim_products value ('13', '挂烫机');
insert into tbl_dim_products value ('14', '料理机');
insert into tbl_dim_products value ('15', '智能电视');
insert into tbl_dim_products value ('16', '波轮洗衣机');
insert into tbl_dim_products value ('17', '滤芯');
insert into tbl_dim_products value ('18', '烟灶系统');
insert into tbl_dim_products value ('19', '烤箱');
insert into tbl_dim_products value ('20', '燃气灶');
insert into tbl_dim_products value ('21', '燃气热水器');
insert into tbl_dim_products value ('22', '电水壶/热水瓶');
insert into tbl_dim_products value ('23', '电热水器');
insert into tbl_dim_products value ('24', '电磁炉');
insert into tbl_dim_products value ('25', '电风扇');
insert into tbl_dim_products value ('26', '电饭煲');
insert into tbl_dim_products value ('27', '破壁机');
insert into tbl_dim_products value ('28', '空气净化器');