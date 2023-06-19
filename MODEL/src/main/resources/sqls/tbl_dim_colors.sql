create table if not exists tbl_dim_colors
(
    id         int(11) not null auto_increment,
    color_name varchar(255) default null,
    primary key (id)
) auto_increment = 21
  default charset = utf8;

insert into tbl_dim_colors VALUES('1','香槟金色');
insert into tbl_dim_colors VALUES ('2','黑色');
insert into tbl_dim_colors VALUES ('3','白色');
insert into tbl_dim_colors VALUES('4','梦境极光【卡其金】');
insert into tbl_dim_colors VALUES ('5','梦境极光【布朗灰】');
insert into tbl_dim_colors VALUES ('6','粉色');
insert into tbl_dim_colors VALUES ('7','金属灰');
insert into tbl_dim_colors VALUES ('8','金色');
insert into tbl_dim_colors VALUES ('9','乐享金');
insert into tbl_dim_colors VALUES ('10','布鲁钢');
insert into tbl_dim_colors VALUES ('11','月光银');
insert into tbl_dim_colors VALUES ('12','时尚光谱【浅金棕】');
insert into tbl_dim_colors VALUES ('13','香槟色');
insert into tbl_dim_colors VALUES ('14','香槟金');
insert into tbl_dim_colors VALUES ('15','灰色');
insert into tbl_dim_colors VALUES ('16','樱花粉');
insert into tbl_dim_colors VALUES ('17','蓝色');
insert into tbl_dim_colors VALUES ('18','金属银');
insert into tbl_dim_colors VALUES ('19','玫瑰金');
insert into tbl_dim_colors VALUES ('20','银色');


