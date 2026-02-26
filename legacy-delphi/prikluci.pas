unit prikluci;

interface

uses
  Winapi.Windows, Winapi.Messages, System.SysUtils, System.Variants, System.Classes, Vcl.Graphics,
  Vcl.Controls, Vcl.Forms, Vcl.Dialogs, Data.DB, Data.Win.ADODB;

type
  TFprikluci = class(TForm)
    ADOQuery1: TADOQuery;
  private
    { Private declarations }
  public
    Function Getprik(mat : string) : integer ;
  end;

var
  Fprikluci: TFprikluci;

implementation

{$R *.dfm}

Function TFprikluci.Getprik(mat : string) : integer ;
  var dir : string ;
begin
  dir := ExtractFilePath(application.ExeName) + '\Montaz_pl.udl';
  Adoquery1.ConnectionString := 'FILE NAME=' + dir ;
  adoquery1.SQL.Clear ;
  adoquery1.SQL.Add('select * from kotni where koda = :MAT') ;
  Adoquery1.Parameters[0].Value := mat ;
  Adoquery1.Parameters[0].name := 'MAT' ;
  Adoquery1.Open ;
  if not Adoquery1.isempty then  result := Adoquery1.FieldByName('id_stroj').Value else result := 0 ;
  adoquery1.Close ;
end;


end.
