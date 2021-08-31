<?php

declare(strict_types=1);

namespace Loouss\Filesystem\Obs;

use League\Flysystem\Config;
use League\Flysystem\FileAttributes;
use League\Flysystem\FilesystemAdapter;
use League\Flysystem\UnableToWriteFile;
use Obs\ObsClient;

class Adapter implements FilesystemAdapter
{
    /**
     * @var ObsClient
     */
    protected ObsClient $client;

    /**
     * @var string
     */
    protected string $bucket;

    /**
     * @param $config  = [
     *    'key' => '*** Provide your Access Key ***',
     *    'secret' => '*** Provide your Secret Key ***',
     *    'endpoint' => 'https://your-endpoint',
     *    'ssl_verify' => false,
     *    'max_retry_count' => 1,
     *    'socket_timeout' => 20,
     *    'connect_timeout' => 20,
     *    'chunk_size' => 8196
     * ]
     */
    public function __construct($config = [])
    {
        $this->bucket = $config['bucket'];
        $key = $config['key'];
        $secret = $config['secret'];
        $endpoint = $config['endpoint'] ?? 'obs.cn-south-1.myhuaweicloud.com';
        $maxRetryCount = $config['maxRetryCount'] ?? 1;
        $timeout = $config['timeout'] ?? 3600;
        $socketTimeout = $config['socketTimeout'] ?? 60;
        $connectTimeout = $config['connectTimeout'] ?? 60;
        $chunkSize = $config['chunk_size'] ?? 8196;

        $this->client = make(ObsClient::class, [
            'key' => $key,
            'secret' => $secret,
            'endpoint' => $endpoint,
            'ssl_verify' => false,
            'max_retry_count' => $maxRetryCount,
            'socket_timeout' => $socketTimeout,
            'connect_timeout' => $connectTimeout,
            'chunk_size' => $chunkSize
        ]);
    }

    public function fileExists(string $path): bool
    {
        return (bool) $this->client->getObjectMetadata(['Bucket' => $this->bucket, 'Key' => $path])['HttpStatusCode'];
    }

    public function write(string $path, string $contents, Config $config): void
    {
        $this->client->putObject(
            [
                'Bucket' => $this->bucket,
                'Key' => $path,
                'Body' => $contents,
            ] //$this->getOssOptions($config)
        );
    }

    public function writeStream(string $path, $contents, Config $config): void
    {
        if (!is_resource($contents)) {
            throw UnableToWriteFile::atLocation($path, 'The contents is invalid resource.');
        }
        $i = 0;
        $bufferSize = 1024 * 1024;
        while (!feof($contents)) {
            if (false === $buffer = fread($contents, $bufferSize)) {
                throw UnableToWriteFile::atLocation($path, 'fread failed');
            }
            $position = $i * $bufferSize;
            $this->client->appendObject($this->bucket, $path, $buffer, $position, $this->getOssOptions($config));
            ++$i;
        }
        fclose($contents);
    }

    public function read(string $path): string
    {
        return $this->client->getObject([
            'Bucket' => $this->bucket,
            'Key' => $path
        ])['Body'];
    }

    public function readStream(string $path)
    {
        return ResourceGenerator::from($this->read($path));
    }

    public function delete(string $path): void
    {
        $this->client->deleteObject([
            'Bucket' => $this->bucket,
            'Key' => $path
        ]);
    }

    public function deleteDirectory(string $path): void
    {
        $lists = $this->listContents($path, true);
        if (!$lists) {
            return;
        }
        $objectList = [];
        foreach ($lists as $value) {
            $objectList[] = [
                'Key' => $value['path'],
                'VersionId' => null
            ];
        }
        $this->client->deleteObjects([
            'Bucket' => $this->bucket,
            // 设置为verbose模式
            'Quiet' => false,
            'Objects' => $objectList
        ]);
    }

    public function createDirectory(string $path, Config $config): void
    {
        $path = substr($path, -1) === '/' ? $path : $path.'/';
        $this->client->putObject([
            'Bucket' => $this->bucket,
            'Key' => $path
        ]);
    }

    public function setVisibility(string $path, string $visibility): void
    {
        $this->client->setObjectAcl(
            [
                'Bucket' => $this->bucket,
                'Key' => $path,
                'ACL' => ($visibility == 'public') ? ObsClient::AclPublicRead : ObsClient::AclPrivate
            ]
        );
    }

    public function visibility(string $path): FileAttributes
    {
        $response = $this->client->getObjectAcl([
            'Bucket' => $this->bucket,
            'Key' => $path
        ]);
        return new FileAttributes($path, null, $response['Permission']);
    }

    public function mimeType(string $path): FileAttributes
    {
        $response = $this->client->getObjectMetadata([
            'Bucket' => $this->bucket, 'Key' => $path
        ]);
        return new FileAttributes($path, null, null, null, $response['Content-Type']);
    }

    public function lastModified(string $path): FileAttributes
    {
        $response = $this->client->getObjectMetadata([
            'Bucket' => $this->bucket, 'Key' => $path
        ]);
        return new FileAttributes($path, null, null, $response['last-modified']);
    }

    public function fileSize(string $path): FileAttributes
    {
        $response = $this->client->getObjectMetadata([
            'Bucket' => $this->bucket, 'Key' => $path
        ]);
        return new FileAttributes($path, $response['ContentLength']);
    }

    public function listContents(string $path, bool $deep): iterable
    {
        $directory = rtrim($path, '\\/');

        $result = [];
        $nextMarker = '';
        while (true) {
            // max-keys 用于限定此次返回object的最大数，如果不设定，默认为100，max-keys取值不能大于1000。
            // prefix   限定返回的object key必须以prefix作为前缀。注意使用prefix查询时，返回的key中仍会包含prefix。
            // delimiter是一个用于对Object名字进行分组的字符。所有名字包含指定的前缀且第一次出现delimiter字符之间的object作为一组元素
            // marker   用户设定结果从marker之后按字母排序的第一个开始返回。
            $options = [
                'Bucket' => $this->bucket,
                'MaxKeys' => 1000,
                'Prefix' => $directory.'/',
                'Delimiter' => '/',
                'Marker' => $nextMarker,
            ];
            $res = $this->client->listObjects($options);

            // 得到nextMarker，从上一次$res读到的最后一个文件的下一个文件开始继续获取文件列表
            $nextMarker = $res->getNextMarker();
            $prefixList = $res->getPrefixList(); // 目录列表
            $objectList = $res->getObjectList(); // 文件列表
            if ($prefixList) {
                foreach ($prefixList as $value) {
                    $result[] = [
                        'type' => 'dir',
                        'path' => $value->getPrefix(),
                    ];
                    if ($deep) {
                        $result = array_merge($result, $this->listContents($value->getPrefix(), $deep));
                    }
                }
            }
            if ($objectList) {
                foreach ($objectList as $value) {
                    if (($value->getSize() === 0) && ($value->getKey() === $directory.'/')) {
                        continue;
                    }
                    $result[] = [
                        'type' => 'file',
                        'path' => $value->getKey(),
                        'timestamp' => strtotime($value->getLastModified()),
                        'size' => $value->getSize(),
                    ];
                }
            }
            if ($nextMarker === '') {
                break;
            }
        }

        return $result;
    }

    public function move(string $source, string $destination, Config $config): void
    {
        $this->client->copyObject([
            'Bucket' => $this->bucket,
            'Key' => $destination,
            'CopySource' => $source,
            'MetadataDirective' => ObsClient::CopyMetadata
        ]);
        $this->client->deleteObject([
            'Bucket' => $this->bucket,
            'Key' => $source
        ]);
    }

    public function copy(string $source, string $destination, Config $config): void
    {
        $this->client->copyObject([
            'Bucket' => $this->bucket,
            'Key' => $destination,
            'CopySource' => $source,
            'MetadataDirective' => ObsClient::CopyMetadata
        ]);
    }

    private function getOssOptions(Config $config): array
    {
        $options = [];
        if ($headers = $config->get('headers')) {
            $options['headers'] = $headers;
        }

        if ($contentType = $config->get('Content-Type')) {
            $options['Content-Type'] = $contentType;
        }

        if ($contentMd5 = $config->get('Content-Md5')) {
            $options['Content-Md5'] = $contentMd5;
            $options['checkmd5'] = false;
        }
        return $options;
    }
}
