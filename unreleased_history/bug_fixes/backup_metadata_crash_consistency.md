Fixed a bug where a power loss or OS crash during `BackupEngine::CreateNewBackup()` with `BackupEngineOptions::sync=true` could leave a corrupt backup instead of no backup.
