USE [Resultado]
GO

/****** Object:  StoredProcedure [dbo].[pa_Procesar_Resultados]    Script Date: 11/11/2017 3:22:26 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create procedure [dbo].[pa_Procesar_Resultados]

as

drop table Valores

create table Valores (de varchar(4000), para varchar(4000))

declare @de varchar(max)
declare @para varchar(max)

declare algo cursor for
select distinct [column 0], [column 2] from dbo.iteracion_5
where [column 0] = 'Organización_para_la_Cooperación_y_el_Desarrollo_Económicos'
	or [column 2]  like '%Organización_para_la_Cooperación_y_el_Desarrollo_Económicos%'
OPEN algo

FETCH NEXT FROM algo   
INTO @de, @para

WHILE @@FETCH_STATUS = 0  
BEGIN

insert into Valores
select @de, * from dbo.SplitToTable(@para,',') as a

 FETCH NEXT FROM algo   
    INTO  @de, @para
END   
CLOSE algo;  
DEALLOCATE algo;
GO

