<?php
/**
 * SQLite storage engine for cache
 *
 *
 * PHP 5
 *
 * CakePHP(tm) : Rapid Development Framework (http://cakephp.org)
 * Copyright (c) Cake Software Foundation, Inc. (http://cakefoundation.org)
 *
 * Licensed under The MIT License
 * For full copyright and license information, please see the LICENSE.txt
 * Sqlitetributions of files must retain the above copyright notice.
 *
 * @copyright     Copyright (c) Cake Software Foundation, Inc. (http://cakefoundation.org)
 * @link          http://cakephp.org CakePHP(tm) Project
 * @package       Cake.Cache.Engine
 * @since         CakePHP(tm) v 2.2
 * @license       http://www.opensource.org/licenses/mit-license.php MIT License
 */
 
App::uses('CacheEngine', 'Cache');
App::uses('CakeLog', 'Log');

/**
 * Sqlite storage engine for cache.
 *
 * @package       Cake.Cache.Engine
 */
class SqliteEngine extends CacheEngine {

/**
 * Sqlite wrapper.
 *
 * @var Sqlite
 */
	protected $_Sqlite = null;

/**
 * Settings
 *
 *  - server = string URL or ip to the Sqlite server host
 *  - port = integer port number to the Sqlite server (default: 6379)
 *  - timeout = float timeout in seconds (default: 0)
 *  - persistent = bool Connects to the Sqlite server with a persistent connection (default: true)
 *
 * @var array
 */
	public $settings = array();

/**
 * Initialize the Cache Engine
 *
 * Called automatically by the cache frontend
 * To reinitialize the settings call Cache::engine('EngineName', [optional] settings = array());
 *
 * @param array $settings array of setting for the engine
 * @return boolean True if the engine has been successfully initialized, false if not
 */
	public function init($settings = array()) {
		parent::init(array_merge(array(
			'engine' => 'Sqlite',
			'prefix' => null,
			'sqlfile' => 'cake_cache.sqlite3',
			'cachetable' => 'cache',
			'autocreatetable' => true,
			'persistent' => true
			), $settings)
		);
    
		if ($return = $this->_connect()) {
			if ($this->settings['autocreatetable']) {
			  $this->_createCacheTable($this->settings['cachetable']);
			}
		}

		return $return;
	}

/**
 * Connects to a Sqlite server
 *
 * @return boolean True if Sqlite server was connected
 */
	protected function _connect() {
		$return = false;
		try {
		if (empty($this->settings['persistent'])) {
			$this->_Sqlite = new PDO('sqlite:'. CACHE . $this->settings['sqlfile'], false, false,array(
				PDO::ATTR_PERSISTENT => true
			));
		} else {
			$this->_Sqlite = new PDO('sqlite:'. CACHE . $this->settings['sqlfile'], false, false,array(
				PDO::ATTR_PERSISTENT => $this->settings['persistent']
			));      
		}
		$return = $this->_Sqlite->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
		
		} catch (PDOException $e) {
			CakeLog::write('debug','Error in connection: '.$e->getMessage());
			return false;
		}
		return $return;
	}
  
	protected function _createCacheTable($tableName){
		$sql="DELETE FROM {$this->settings['cachetable']} WHERE expire>0 AND expire<".time();
		try {
			$this->_Sqlite->exec($sql);
		} catch(Exception $e){
			$sql=<<<EOD
CREATE TABLE $tableName
(
id CHAR(128) PRIMARY KEY,
expire INTEGER,
value $blob
)
EOD;
		$this->_Sqlite->exec($sql);
		}
	
	}
  
/**
* Removes the expired data values.
*/
	public function gc(){
		$this->_Sqlite->exec("DELETE FROM {$this->cacheTableName} WHERE expire>0 AND expire<".time());
	}

/**
 * Write data for key into cache.
 *
 * @param string $key Identifier for the data
 * @param mixed $value Data to be cached
 * @param integer $duration How long to cache the data, in seconds
 * @return boolean True if the data was successfully cached, false on failure
 */
	public function write($key, $value, $duration) {
		if (!is_int($value)) {
			$value = serialize($value);
		}
		if ($duration > 0) {
			$expires = time() + $duration;
		} else {
			$expires = 0;
		}
		if ($this->read($key) !== false) {
			$sql="UPDATE {$this->settings['cachetable']} set value=:value, expire={$expires} WHERE id='$key'";
		} else {
			$sql="INSERT INTO {$this->settings['cachetable']} (id,expire,value) VALUES ('$key', $expires, :value)";
		}
		try {
			$command = $this->_Sqlite->prepare($sql);
      			$command->bindValue(':value',$value, PDO::PARAM_LOB);
      			$return = $command->execute();
		} catch (Exception $e) {
			CakeLog::write('debug','Error in write: '.$e->getMessage());
			$return = false;
		}
    		return $return;
	}

/**
 * Read a key from the cache
 *
 * @param string $key Identifier for the data
 * @return mixed The cached data, or false if the data doesn't exist, has expired, or if there was an error fetching it
 */
	public function read($key) {
		$time=time();
		$sql="SELECT value FROM {$this->settings['cachetable']} WHERE id='$key' AND (expire=0 OR expire>$time)";
		$command = $this->_Sqlite->prepare($sql);
		$value = $command->execute();
		if ($value) {
			$value = $command->fetchColumn();
			if (ctype_digit($value)) {
				$value = (int)$value;
			}
			if ($value !== false && is_string($value)) {
				$value = unserialize($value);
			}
		}
		return $value;
	}

/**
 * Increments the value of an integer cached key
 *
 * @param string $key Identifier for the data
 * @param integer $offset How much to increment
 * @return New incremented value, false otherwise
 * @throws CacheException when you try to increment with compress = true
 */
	public function increment($key, $offset = 1) {
		if (ctype_digit($offset)) {
			$offset = (int)$offset;
			$sql="UPDATE {$this->settings['cachetable']} set value = value + {$offset} WHERE id='$key'";
			$command = $this->_Sqlite->prepare($sql);
			if($return = $command->execute()){
				$return = $this->read($key);
			}
		} else {
			throw new CacheException(__d('cake_dev', 'Decrement offset is not of type int.'));
		}
		
		return $return;
	}

/**
 * Decrements the value of an integer cached key
 *
 * @param string $key Identifier for the data
 * @param integer $offset How much to subtract
 * @return New decremented value, false otherwise
 * @throws CacheException when you try to decrement with compress = true
 */
	public function decrement($key, $offset = 1) {
		if (ctype_digit($offset)) {
			$offset = (int)$offset;
			$sql="UPDATE {$this->settings['cachetable']} set value = value - {$offset} WHERE id='$key'";
			$command = $this->_Sqlite->prepare($sql);
			if ($return = $command->execute()) {
				$return = $this->read($key);
			}
		} else {
			throw new CacheException(__d('cake_dev', 'Decrement offset is not of type int.'));
		}
		
		return $return;
	}

/**
 * Delete a key from the cache
 *
 * @param string $key Identifier for the data
 * @return boolean True if the value was successfully deleted, false if it didn't exist or couldn't be removed
 */
	public function delete($key) {
		$sql="DELETE FROM {$this->settings['cachetable']} WHERE id='$key'";
		return $this->_Sqlite->exec($sql) > 0;
	}

/**
 * Delete all keys from the cache
 *
 * @param boolean $check
 * @return boolean True if the cache was successfully cleared, false otherwise
 */
	public function clear($check) {
		if ($check) {
			return true;
		}
		$sql="DELETE FROM {$this->settings['cachetable']}";

		return $this->_Sqlite->exec($sql) > 0;
	}

/**
 * Returns the `group value` for each of the configured groups
 * If the group initial value was not found, then it initializes
 * the group accordingly.
 *
 * @return array
 */
	public function groups() {
		$result = array();
		foreach ($this->settings['groups'] as $group) {
			$value = $this->read($this->settings['prefix'] . $group);
			if (!$value) {
				$value = 1;
				$this->write($this->settings['prefix'] . $group, $value);
			}
			$result[] = $group . $value;
		}
		return $result;
	}

/**
 * Increments the group value to simulate deletion of all keys under a group
 * old values will remain in storage until they expire.
 *
 * @return boolean success
 */
	public function clearGroup($group) {
		return (bool)$this->increment($this->settings['prefix'] . $group);
	}

}
