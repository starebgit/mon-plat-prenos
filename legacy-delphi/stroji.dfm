object Fstroji: TFstroji
  Left = 0
  Top = 0
  Caption = 'Fstroji'
  ClientHeight = 412
  ClientWidth = 852
  Color = clBtnFace
  Font.Charset = DEFAULT_CHARSET
  Font.Color = clWindowText
  Font.Height = -11
  Font.Name = 'Tahoma'
  Font.Style = []
  OldCreateOrder = False
  PixelsPerInch = 96
  TextHeight = 13
  object DataSource1: TDataSource
    DataSet = ADOQuery1
    Left = 224
    Top = 112
  end
  object ADOQuery1: TADOQuery
    Parameters = <>
    Left = 320
    Top = 120
  end
end
