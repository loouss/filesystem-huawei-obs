<?php

declare(strict_types=1);

namespace Loouss\Filesystem\Obs;

use GuzzleHttp\Exception\RequestException;
use League\Flysystem\Config;
use League\Flysystem\FileAttributes;
use League\Flysystem\FilesystemAdapter;
use League\Flysystem\UnableToCheckFileExistence;
use League\Flysystem\UnableToCopyFile;
use League\Flysystem\UnableToCreateDirectory;
use League\Flysystem\UnableToDeleteDirectory;
use League\Flysystem\UnableToDeleteFile;
use League\Flysystem\UnableToMoveFile;
use League\Flysystem\UnableToReadFile;
use League\Flysystem\UnableToSetVisibility;
use League\Flysystem\UnableToWriteFile;
use Loouss\ObsClient\ObjectClient;

class Adapter implements FilesystemAdapter
{

    protected ObjectClient $client;

    /**
     * @var string
     */
    protected string $bucket;

    /**
     * @param  array  $config
     */
    public function __construct(array $config = [])
    {
        $this->bucket = $config['bucket'];
        $key = $config['key'];
        $secret = $config['secret'];
        $endpoint = $config['endpoint'] ?? 'obs.cn-south-1.myhuaweicloud.com';

        $this->client = new ObjectClient($key, $secret, $endpoint, $this->bucket);
    }

    public function fileExists(string $path): bool
    {
        try {
            return (bool) $this->client->headObject($path);
        } catch (RequestException $exception) {
            if ($exception->hasResponse()) {
                if ($exception->getResponse()->getStatusCode() == 404) {
                    return false;
                }
            } else {
                return false;
            }

            return true;
        }
    }

    public function write(string $path, string $contents, Config $config): void
    {
        try {
            $response = $this->client->putObject($path, $contents, $config->get('headers', []));
            if ($response->getStatusCode() != 200) {
                throw UnableToWriteFile::atLocation($path, (string) $response->getBody());
            }
        } catch (RequestException $exception) {
            throw UnableToWriteFile::atLocation($path, (string) $exception->getResponse()->getBody());
        }
    }

    public function writeStream(string $path, $contents, Config $config): void
    {
        $this->write($path, \stream_get_contents($contents), $config);
    }

    public function read(string $path): string
    {
        try {
            $response = $this->client->getObject($path);
            return (string) $response->getBody();
        } catch (RequestException $exception) {
            throw  UnableToReadFile::fromLocation($path, $exception->getResponse()->getBody()->getContents());
        }
    }

    public function readStream(string $path)
    {
        return ResourceGenerator::from($this->read($path));
    }

    public function delete(string $path): void
    {
        try {
            $response = $this->client->deleteObject($path);
            if ($response->getStatusCode() != 200) {
                throw UnableToDeleteFile::atLocation($path, (string) $response->getBody());
            }
        } catch (RequestException $exception) {
            throw UnableToDeleteFile::atLocation($path, (string) $exception->getResponse()->getBody());
        }
    }

    public function deleteDirectory(string $path): void
    {
        throw new UnableToDeleteDirectory();
    }

    public function createDirectory(string $path, Config $config): void
    {
        $path = substr($path, -1) === '/' ? $path : $path.'/';
        try {
            $response = $this->client->putObject($path, '', $config->get('headers', []));
            if ($response->getStatusCode() != 200) {
                throw UnableToCreateDirectory::atLocation($path, (string) $response->getBody());
            }
        } catch (RequestException $exception) {
            throw UnableToCreateDirectory::atLocation($path, (string) $exception->getResponse()->getBody());
        }
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
        throw UnableToSetVisibility::atLocation($path);
    }

    public function mimeType(string $path): FileAttributes
    {
        $response = $this->client->headObject($path);
        return new FileAttributes($path, null, null, null, $response->getHeaderLine('Content-Type'));
    }

    public function lastModified(string $path): FileAttributes
    {
        $response = $this->client->headObject($path);
        return new FileAttributes($path, null, null, $response->getHeaderLine('last-modified'));
    }

    public function fileSize(string $path): FileAttributes
    {
        $response = $this->client->headObject($path);
        return new FileAttributes($path, $response->getHeaderLine('Content-Length'));
    }

    public function listContents(string $path, bool $deep): iterable
    {
        throw new UnableToCheckFileExistence();
    }

    public function move(string $source, string $destination, Config $config): void
    {
        throw new UnableToMoveFile();
    }

    public function copy(string $source, string $destination, Config $config): void
    {
        throw new UnableToCopyFile();
    }

}
