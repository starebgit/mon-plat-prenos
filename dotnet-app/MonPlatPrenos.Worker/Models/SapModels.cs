using System;
namespace MonPlatPrenos.Worker.Models;

public sealed record SapOrderHeader(
    string OrderNumber,
    string Material,
    string Status,
    int PlannedQuantity,
    DateTime StartDate,
    string WorkCenterTrackCode,
    string SchedulerCode,
    string Plant);

public sealed record SapOperation(
    string OrderNumber,
    string Confirmation,
    string OperationCode,
    int ConfirmableQty,
    string StepCode,
    string WorkCenterCode);

public sealed record SapConfirmation(
    string Confirmation,
    string Counter,
    int Yield);

public sealed record SapComponent(
    string OrderNumber,
    string Material,
    string Description);

public sealed record PlateDemandRecord(
    int Track,
    int? Stev,
    string OrderNumber,
    string Material,
    int Quantity,
    DateTime StartDate,
    DateTime? Dan,
    int? Izmena);

public sealed record UnifiedItem(
    string OrderNumber,
    string PlateMaterial,
    string ComponentMaterial,
    string ComponentDescription,
    string Category,
    int? Zap,
    int RequiredQty,
    DateTime CapturedAtUtc);

public sealed record SemiFinishedTrace(
    string PlateOrder,
    string PlateMaterial,
    string Category,
    string SemiMaterial,
    string SemiOrder,
    int AfruYieldDelta,
    DateTime CapturedAtUtc);
