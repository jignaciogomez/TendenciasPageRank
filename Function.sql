USE [Resultado]
GO

/****** Object:  UserDefinedFunction [dbo].[SplitToTable]    Script Date: 11/11/2017 3:22:50 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE FUNCTION [dbo].[SplitToTable]
	(
		@cadena as varchar(4000), @Delimitador varchar(1)
	)
	
returns @ValueTable table ([Value] nvarchar(4000))

AS

begin
 declare @NextString nvarchar(4000)
 declare @Pos int
 declare @NextPos int
 declare @CommaCheck nvarchar(1)
  
 --Inicializa
 set @NextString = ''
 set @CommaCheck = right(@cadena,1) 
  
 set @cadena = @cadena + @Delimitador
  
 --Busca la posición del primer delimitador
 set @Pos = charindex(@Delimitador,@cadena)
 set @NextPos = 1
  
 --Itera mientras exista un delimitador en el string
 while (@pos <>  0)  
 begin
  set @NextString = substring(@cadena,1,@Pos - 1)
  
  insert into @ValueTable ( [Value]) Values (@NextString)
  
  set @cadena = substring(@cadena,@pos +1,len(@cadena))
   
  set @NextPos = @Pos
  set @pos  = charindex(@Delimitador,@cadena)
 end
 return
end

GO

