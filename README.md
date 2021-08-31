# HuaWei OBS Adapter for Flysystem

## 使用

```php
<?php

$adapter = new Adapter([
    'key' => '',
    'secret' => '',
    'endpoint' => '',
    'ssl_verify' => false,
    'max_retry_count' => '',
    'socket_timeout' => '',
    'connect_timeout' => '',
    'chunk_size' => ''
]);
$flysystem = new Filesystem($adapter);
$flysystem->write('test.json', Json::encode(['id' => uniqid()]));
```
