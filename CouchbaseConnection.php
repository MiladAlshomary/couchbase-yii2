<?php
namespace miladalsh\couchbase;

use yii\base\Component;
use yii\db\Exception;
use yii\helpers\Inflector;

/**
 * @author Milad Alshomary
 */
class CouchbaseConnection extends Component
{
    /**
     * @var string the connectionString for connecting to the couchbase server.
     */
    public $connectionString = '';

    /**
     * @var integer the port to use for connecting to the redis server. Default port is 6379.
     * If [[unixSocket]] is specified, hostname and port will be ignored.
     */
    public $port = 6379;
 
    /**
     * @var string the username for establishing couchbase connection.
    */
    public $username;

    /**
     * @var string the password for establishing couchbase connection.
     */
    public $password;
 
    /**
     * @var resource couchbase socket connection
     */
    private $_cluster;


    /**
    * @var array of opened buckets to couchbase server
    **/
    private $_buckets;

    /**
     * Closes the connection when this component is being serialized.
     * @return array
     */
    public function __sleep()
    {
        $this->close();
        return array_keys(get_object_vars($this));
    }

    /**
     * Returns a value indicating whether the DB connection is established.
     * @return boolean whether the DB connection is established
     */
    public function getIsActive()
    {
        return $this->_cluster !== null;
    }

    /**
     * Establishes a DB connection.
     * It does nothing if a DB connection has already been established.
     * @throws Exception if connection fails
     */
    public function open()
    {
        if($this->_cluster === null) {
            
            if(empty($this->connectionString)) {
                throw new CException('CCouchbaseConnection.connectionString cannot be empty.');
            }

            try {
                // Connect to Couchbase Server
                $this->_cluster = new \CouchbaseCluster($this->connectionString, $this->username, $this->password);             
                if (!is_object($this->_cluster)) {
                    throw new CException('CConnection failed to open the Couchbase connection');
                }
            } catch(Exception $e) {
                throw new CException('CConnection failed to open the Couchbase connection' . $e->getMessage());
            }
        }
    }

    /**
     * retrieve bucket instance if its already opened or initialize it and return it
     */
    public function getBucket($bucket_name, $password = '') {
        if(!empty($this->_buckets[$bucket_name])) {
            return $this->_buckets[$bucket_name];
        }

        try {
            $this->_buckets[$bucket_name] = $this->_cluster->openBucket($bucket_name, $password);
        } catch(CouchbaseException $e) {
            Yii::warning('Exception in opening bukcet : ' . $e->getMessage());
        }

        return $this->_buckets[$bucket_name];

    }

    /**
     * Closes the currently active connection.
     * It does nothing if the connection is already closed.
     */
    public function close()
    {
        if ($this->_cluster !== null) {
           $this->_cluster = null;
           $this->_buckets = [];
        }
    }
}
