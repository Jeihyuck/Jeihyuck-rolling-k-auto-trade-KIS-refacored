$ErrorActionPreference = "Stop"
function ConvertTo-NullimDayMask {
  param([object]$DaysOfWeek)
  if($null -eq $DaysOfWeek){return 0}
  if($DaysOfWeek -is [System.Enum] -or $DaysOfWeek -is [int] -or $DaysOfWeek -is [uint16] -or $DaysOfWeek -is [uint32]){return [int]$DaysOfWeek}
  $mask=0
  foreach($raw in @($DaysOfWeek)){
    foreach($day in ("$raw" -split '[, ]+' | Where-Object { $_ })){
      switch($day){
        'Sunday'{$mask=$mask -bor 1};'Monday'{$mask=$mask -bor 2};'Tuesday'{$mask=$mask -bor 4};'Wednesday'{$mask=$mask -bor 8};
        'Thursday'{$mask=$mask -bor 16};'Friday'{$mask=$mask -bor 32};'Saturday'{$mask=$mask -bor 64};
        default{if($day -match '^\d+$'){$mask=$mask -bor [int]$day}else{throw "Unknown DaysOfWeek value: $day"}}
      }
    }
  }
  return $mask
}
$krTrigger=New-ScheduledTaskTrigger -Weekly -WeeksInterval 1 -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At "06:30"
$usEveningTrigger=New-ScheduledTaskTrigger -Weekly -WeeksInterval 1 -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At "22:30"
$usMorningTrigger=New-ScheduledTaskTrigger -Weekly -WeeksInterval 1 -DaysOfWeek Tuesday,Wednesday,Thursday,Friday,Saturday -At "07:00"
$contracts=@(
  [pscustomobject]@{Trigger=$krTrigger;Expected=62;Label='KR'},
  [pscustomobject]@{Trigger=$usEveningTrigger;Expected=62;Label='US_EVENING'},
  [pscustomobject]@{Trigger=$usMorningTrigger;Expected=124;Label='US_MORNING'}
)
foreach($contract in $contracts){
  $trigger=$contract.Trigger;$expected=[int]$contract.Expected;$label=$contract.Label;$actual=ConvertTo-NullimDayMask $trigger.DaysOfWeek
  if($actual -ne $expected){throw "$label mask expected=$expected actual=$actual raw=$($trigger.DaysOfWeek)"}
  if([int]$trigger.WeeksInterval -ne 1){throw "$label WeeksInterval must be 1"}
  if(($actual -band 1) -ne 0){throw "$label must not contain Sunday"}
}
$usMask=ConvertTo-NullimDayMask $usMorningTrigger.DaysOfWeek
if(($usMask -band 64) -eq 0){throw 'US morning must contain Saturday'}
if(($usMask -band 2) -ne 0){throw 'US morning must not contain Monday'}
Write-Host '[SCHEDULER_TRIGGER_CONTRACT][OK] kr=62 us_evening=62 us_morning=124'
