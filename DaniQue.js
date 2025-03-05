/***********************************************************
|* Author: Naaba Technologies LLC. Columbus Ohio USA
|* Author Website: https://naabatechs.com/
|* Credits: Niel(ADA) Technologies Inc.
/* Project Name: DANIQUE
/* Project Description: A high-performance, configurable task execution management system.
|* Repos: https://github.com/naabatechs/danique
|*********************| Version: .v2.0 |*******************
|* Licensed Under MIT
|* This project is provided as is with no copyright infringement
|* of any kind. Deal with it however you want.
|* We do apologize for but do not take responsibility for the
|* damages caused thereafter using this artwork.
|* License: https://naabatechs.com/license/freelicence
************************************************************/

/**
 * Features:
 * - Prioritized task execution
 * - Concurrency control
 * - Timeout handling
 * - Automatic retries
 * - Pause/resume functionality
 * - Sequential mode
 * - Stop-on-error option
 * - JSON input/output support
 * - Performance optimizations
 * - Comprehensive error handling
 * - Event hooks
 * - Concurrency Enhancement
 * - Graceful Shutdown Support
 * - TypeScript Support (via JSDoc)
 * - Memory Management
 * - Task Persistence
 * - Task Progress Tracking
 * - Task Dependencies
 * - Rate Limiting
 * - Task Cancellation
 */

/**
 * @typedef {Object} DANIQUEConfig
 * @property {number} [concurrency=1] - Maximum number of concurrent tasks
 * @property {boolean} [sequential=false] - Run tasks sequentially
 * @property {boolean} [stopOnError=false] - Stop queue on first error
 * @property {number} [defaultTimeout=0] - Default timeout for all tasks (ms)
 * @property {number} [defaultRetries=0] - Default retry count for all tasks
 * @property {number} [rateLimitPerSecond=0] - Maximum tasks per second (0 for unlimited)
 * @property {boolean} [persistTasks=false] - Whether to persist tasks
 * @property {string} [persistenceKey='danique-tasks'] - Key for task persistence
 * @property {Function} [onTaskStart] - Called when a task starts
 * @property {Function} [onTaskSuccess] - Called when a task succeeds
 * @property {Function} [onTaskError] - Called when a task fails
 * @property {Function} [onTaskProgress] - Called when a task reports progress
 * @property {Function} [onQueueEmpty] - Called when the queue becomes empty
 * @property {Function} [onQueueDrained] - Called when all tasks are completed
 * @property {Function} [onMemoryWarning] - Called when memory usage exceeds threshold
 * @property {number} [memoryWarningThreshold=85] - Memory usage percentage threshold
 * @property {boolean} [autoCleanup=true] - Automatically clean up completed tasks
 * @property {string} [storageType='localStorage'] - Storage type for persistence
 */

/**
 * @typedef {Object} TaskOptions
 * @property {Function} [task] - Task function
 * @property {number} [priority=0] - Task priority (higher numbers = higher priority)
 * @property {number} [timeout=0] - Task timeout in milliseconds
 * @property {number} [retries=0] - Number of retries for the task
 * @property {Array} [args=[]] - Arguments to pass to the task
 * @property {boolean} [jsonInput=false] - Parse arguments as JSON
 * @property {boolean} [jsonOutput=false] - Convert result to JSON
 * @property {string} [id] - Unique task identifier
 * @property {string[]} [depends=[]] - Array of task IDs this task depends on
 * @property {Object} [metadata={}] - Custom metadata for the task
 */

/**
 * @typedef {Object} TaskStatus
 * @property {string} id - Task ID
 * @property {string} status - Task status (pending, running, completed, failed)
 * @property {number} progress - Task progress (0-100)
 * @property {number} priority - Task priority
 * @property {Array} dependencies - Task dependencies
 * @property {Object} metadata - Task metadata
 * @property {Date} createdAt - Task creation timestamp
 * @property {Date} [startedAt] - Task start timestamp
 * @property {Date} [completedAt] - Task completion timestamp
 */

/**
 * @typedef {Object} QueueStatus
 * @property {number} pending - Number of pending tasks
 * @property {number} running - Number of running tasks
 * @property {boolean} isPaused - Whether the queue is paused
 * @property {boolean} isStopped - Whether the queue is stopped
 * @property {boolean} hasError - Whether an error occurred
 * @property {number} concurrency - Current concurrency
 * @property {number} tasksCompleted - Number of completed tasks
 * @property {number} tasksFailed - Number of failed tasks
 * @property {number} memoryUsage - Current memory usage percentage
 * @property {boolean} isShuttingDown - Whether the queue is shutting down
 */

class DANIQUE {
    /**
     * Create a new DANIQUE instance
     * @param {DANIQUEConfig} config - Configuration options
     */
    constructor(config = {}) {
        // Default configuration with object destructuring for speed
        const {
            concurrency = 1,
            sequential = false,
            stopOnError = false,
            defaultTimeout = 0,
            defaultRetries = 0,
            rateLimitPerSecond = 0,
            persistTasks = false,
            persistenceKey = 'danique-tasks',
            onTaskStart,
            onTaskSuccess,
            onTaskError,
            onTaskProgress,
            onQueueEmpty,
            onQueueDrained,
            onMemoryWarning,
            memoryWarningThreshold = 85,
            autoCleanup = true,
            storageType = 'localStorage'
        } = config;

        this.concurrency = sequential ? 1 : concurrency;
        this.sequential = sequential;
        this.stopOnError = stopOnError;
        this.defaultTimeout = defaultTimeout;
        this.defaultRetries = defaultRetries;
        this.rateLimitPerSecond = rateLimitPerSecond;
        this.persistTasks = persistTasks;
        this.persistenceKey = persistenceKey;
        this.memoryWarningThreshold = memoryWarningThreshold;
        this.autoCleanup = autoCleanup;
        this.storageType = storageType;

        // Task management
        this.running = 0;
        this.queue = [];
        this.completedTasks = [];
        this.failedTasks = [];
        this.taskMap = new Map(); // For O(1) lookup by ID
        this.dependencyGraph = new Map(); // For task dependencies
        this.taskProgress = new Map(); // For task progress
        this.isPaused = false;
        this.isStopped = false;
        this.isShuttingDown = false;
        this.hasError = false;
        this.tasksCompleted = 0;
        this.tasksFailed = 0;
        this.rateLimit = {
            lastCheck: Date.now(),
            count: 0
        };

        // Event hooks using arrow functions to preserve context
        this.onTaskStart = onTaskStart;
        this.onTaskSuccess = onTaskSuccess;
        this.onTaskError = onTaskError;
        this.onTaskProgress = onTaskProgress;
        this.onQueueEmpty = onQueueEmpty;
        this.onQueueDrained = onQueueDrained;
        this.onMemoryWarning = onMemoryWarning;

        // Performance optimization - cache frequently used methods
        this._next = this._next.bind(this);
        this._executeTask = this._executeTask.bind(this);
        this._handleTaskSuccess = this._handleTaskSuccess.bind(this);
        this._handleTaskError = this._handleTaskError.bind(this);
        this._checkMemoryUsage = this._checkMemoryUsage.bind(this);
        
        // Setup memory monitoring
        if (this.onMemoryWarning) {
            this.memoryCheckInterval = setInterval(this._checkMemoryUsage, 30000);
        }
        
        // Load persisted tasks if enabled
        if (this.persistTasks) {
            this._loadPersistedTasks();
        }
        
        // Setup shutdown handler
        if (typeof window !== 'undefined') {
            window.addEventListener('beforeunload', this._handleBeforeUnload.bind(this));
        }
        
        // Setup cleanup interval for completed tasks
        if (this.autoCleanup) {
            this.cleanupInterval = setInterval(this._cleanupCompletedTasks.bind(this), 60000);
        }
    }

    /**
     * Add a task to the queue
     * @param {Function|TaskOptions} task - Task function or object with task configuration
     * @param {TaskOptions} [options] - Task options (if task is a function)
     * @returns {Promise} Promise that resolves when the task completes
     */
    add(task, options = {}) {
        // Handle task being passed as object with task property
        if (typeof task === 'object' && task !== null && typeof task.task === 'function') {
            options = { ...task };
            task = options.task;
            delete options.task;
        }

        // Ensure task is a function
        if (typeof task !== 'function') {
            throw new TypeError('Task must be a function');
        }

        // Handle memory pressure
        if (this._checkMemoryUsage() > this.memoryWarningThreshold) {
            return Promise.reject(new Error('Memory threshold exceeded, task rejected'));
        }

        // Ensure task has a unique ID
        const taskId = options.id || this._generateTaskId();
        
        // Check if task with this ID already exists
        if (this.taskMap.has(taskId)) {
            return Promise.reject(new Error(`Task with ID ${taskId} already exists`));
        }

        // Create a promise that will resolve/reject when the task completes
        let taskResolve, taskReject;
        const taskPromise = new Promise((resolve, reject) => {
            taskResolve = resolve;
            taskReject = reject;
        });

        // Create task wrapper with promise handlers
        const taskWrapper = {
            fn: task,
            priority: options.priority ?? 0,
            timeout: options.timeout ?? this.defaultTimeout,
            retries: options.retries ?? this.defaultRetries,
            retriesLeft: options.retries ?? this.defaultRetries,
            id: taskId,
            resolve: taskResolve,
            reject: taskReject,
            jsonInput: options.jsonInput ?? false,
            jsonOutput: options.jsonOutput ?? false,
            args: options.args ?? [],
            depends: options.depends ?? [],
            metadata: options.metadata ?? {},
            status: 'pending',
            progress: 0,
            createdAt: new Date(),
            startedAt: null,
            completedAt: null,
            wasCancelled: false,
            // Add progress reporting function
            reportProgress: (percent) => {
                this._updateTaskProgress(taskId, percent);
            }
        };

        // Add to task map for O(1) lookup
        this.taskMap.set(taskId, taskWrapper);
        
        // Handle dependencies
        if (taskWrapper.depends.length > 0) {
            this._addTaskDependencies(taskWrapper);
            
            // If task has dependencies, don't add to queue yet
            // It will be added once all dependencies are completed
            return taskPromise;
        }

        // Fast insertion based on priority using binary insertion
        this._insertWithPriority(taskWrapper);
        
        // Persist task if enabled
        if (this.persistTasks) {
            this._persistTasks();
        }
        
        // Start processing if not paused or stopped
        if (!this.isPaused && !this.isStopped) {
            // Use microtask queue for better performance
            queueMicrotask(() => this._next());
        }

        return taskPromise;
    }

    /**
     * Add multiple tasks to the queue
     * @param {Array<Function|TaskOptions>} tasks - Array of tasks or task configurations
     * @param {TaskOptions} [defaultOptions] - Default options for all tasks
     * @returns {Promise<Array>} Promise that resolves with all task results
     */
    addAll(tasks, defaultOptions = {}) {
        if (!Array.isArray(tasks)) {
            throw new TypeError('Tasks must be an array');
        }

        const promises = tasks.map(task => {
            if (typeof task === 'function') {
                return this.add(task, defaultOptions);
            } else {
                return this.add({ ...defaultOptions, ...task });
            }
        });

        return Promise.all(promises);
    }

    /**
     * Pause the queue (stops processing new tasks)
     * @returns {DANIQUE} This queue instance for chaining
     */
    pause() {
        this.isPaused = true;
        return this;
    }

    /**
     * Resume the queue
     * @returns {DANIQUE} This queue instance for chaining
     */
    resume() {
        if (this.isPaused) {
            this.isPaused = false;
            
            // Start processing tasks immediately
            for (let i = 0; i < this.concurrency; i++) {
                this._next();
            }
        }
        return this;
    }

    /**
     * Clear all pending tasks from the queue
     * @param {boolean} [rejectPending=true] - Whether to reject pending tasks
     * @param {*} [reason] - Rejection reason
     * @returns {DANIQUE} This queue instance for chaining
     */
    clear(rejectPending = true, reason = new Error('Queue cleared')) {
        const pendingTasks = [...this.queue];
        this.queue = [];
        
        if (rejectPending) {
            pendingTasks.forEach(task => {
                task.reject(reason);
                this.taskMap.delete(task.id);
            });
        }
        
        // Update persistence if enabled
        if (this.persistTasks) {
            this._persistTasks();
        }
        
        return this;
    }

    /**
     * Stop the queue (stops processing and rejects all pending tasks)
     * @param {*} [reason] - Rejection reason
     * @returns {DANIQUE} This queue instance for chaining
     */
    stop(reason = new Error('Queue stopped')) {
        this.isStopped = true;
        this.clear(true, reason);
        return this;
    }

    /**
     * Initiate graceful shutdown
     * @param {number} [timeout=5000] - Maximum time to wait for running tasks in ms
     * @returns {Promise} Promise that resolves when shutdown is complete
     */
    shutdown(timeout = 5000) {
        if (this.isShuttingDown) {
            return Promise.reject(new Error('Shutdown already in progress'));
        }
        
        this.isShuttingDown = true;
        this.isPaused = true;
        
        logger.info(`Initiating graceful shutdown (timeout: ${timeout}ms)`);
        
        // Clear all pending tasks
        this.clear(true, new Error('Queue shutting down'));
        
        // Wait for running tasks to complete
        return new Promise((resolve) => {
            const checkShutdown = () => {
                if (this.running === 0) {
                    this._cleanup();
                    resolve();
                }
            };
            
            checkShutdown();
            
            // Set a timeout in case tasks don't complete
            if (this.running > 0) {
                const timer = setTimeout(() => {
                    logger.warn(`Shutdown timeout reached with ${this.running} tasks still running`);
                    this._cleanup();
                    resolve();
                }, timeout);
                
                // Poll for completion
                const interval = setInterval(() => {
                    if (this.running === 0) {
                        clearTimeout(timer);
                        clearInterval(interval);
                        this._cleanup();
                        resolve();
                    }
                }, 100);
            }
        });
    }

    /**
     * Cancel a specific task by ID
     * @param {string} taskId - ID of the task to cancel
     * @param {*} [reason] - Rejection reason
     * @returns {boolean} True if task was found and cancelled
     */
    cancelTask(taskId, reason = new Error('Task cancelled')) {
        // Check if task is in the queue
        const taskIndex = this.queue.findIndex(task => task.id === taskId);
        if (taskIndex >= 0) {
            const task = this.queue[taskIndex];
            task.wasCancelled = true;
            task.reject(reason);
            this.queue.splice(taskIndex, 1);
            this.taskMap.delete(taskId);
            return true;
        }
        
        // If task is running, mark as cancelled but can't stop it
        if (this.taskMap.has(taskId)) {
            const task = this.taskMap.get(taskId);
            // Only mark as cancelled if it's not already completed
            if (task.status === 'running') {
                task.wasCancelled = true;
                return true;
            }
        }
        
        return false;
    }

    /**
     * Update task priority
     * @param {string} taskId - ID of the task
     * @param {number} newPriority - New priority for the task
     * @returns {boolean} True if task was found and priority updated
     */
    updatePriority(taskId, newPriority) {
        // Check if task is in the queue
        const taskIndex = this.queue.findIndex(task => task.id === taskId);
        if (taskIndex >= 0) {
            // Remove from queue
            const task = this.queue[taskIndex];
            this.queue.splice(taskIndex, 1);
            
            // Update priority
            task.priority = newPriority;
            
            // Re-insert with new priority
            this._insertWithPriority(task);
            
            // Update persistence if enabled
            if (this.persistTasks) {
                this._persistTasks();
            }
            
            return true;
        }
        
        return false;
    }

    /**
     * Get the current status of the queue
     * @returns {QueueStatus} Status object
     */
    getStatus() {
        return {
            pending: this.queue.length,
            running: this.running,
            isPaused: this.isPaused,
            isStopped: this.isStopped,
            hasError: this.hasError,
            concurrency: this.concurrency,
            tasksCompleted: this.tasksCompleted,
            tasksFailed: this.tasksFailed,
            memoryUsage: this._checkMemoryUsage(),
            isShuttingDown: this.isShuttingDown
        };
    }

    /**
     * Get the status of a specific task
     * @param {string} taskId - ID of the task
     * @returns {TaskStatus|null} Task status object or null if not found
     */
    getTaskStatus(taskId) {
        if (!this.taskMap.has(taskId)) {
            return null;
        }
        
        const task = this.taskMap.get(taskId);
        return {
            id: task.id,
            status: task.status,
            progress: task.progress,
            priority: task.priority,
            dependencies: task.depends,
            metadata: task.metadata,
            createdAt: task.createdAt,
            startedAt: task.startedAt,
            completedAt: task.completedAt
        };
    }

    /**
     * Get status of all tasks (running, pending, completed, failed)
     * @returns {Object} Object with arrays of task statuses by category
     */
    getAllTaskStatus() {
        return {
            pending: this.queue.map(task => this.getTaskStatus(task.id)),
            running: Array.from(this.taskMap.values())
                .filter(task => task.status === 'running')
                .map(task => this.getTaskStatus(task.id)),
            completed: this.completedTasks.slice(-100).map(taskId => this.getTaskStatus(taskId)).filter(Boolean),
            failed: this.failedTasks.slice(-100).map(taskId => this.getTaskStatus(taskId)).filter(Boolean)
        };
    }

    /**
     * Change the concurrency level
     * @param {number} newConcurrency - New concurrency level
     * @returns {DANIQUE} This queue instance for chaining
     */
    setConcurrency(newConcurrency) {
        if (typeof newConcurrency !== 'number' || newConcurrency < 1) {
            throw new TypeError('Concurrency must be a positive number');
        }
        
        const oldConcurrency = this.concurrency;
        this.concurrency = newConcurrency;
        
        logger.info(`Concurrency changed from ${oldConcurrency} to ${newConcurrency}`);
        
        // If increasing concurrency, start processing more tasks
        if (newConcurrency > oldConcurrency && !this.isPaused && !this.isStopped) {
            for (let i = 0; i < newConcurrency - oldConcurrency; i++) {
                this._next();
            }
        }
        
        return this;
    }

    /**
     * Clean up resources
     * @private
     */
    _cleanup() {
        // Clear intervals
        if (this.memoryCheckInterval) {
            clearInterval(this.memoryCheckInterval);
        }
        
        if (this.cleanupInterval) {
            clearInterval(this.cleanupInterval);
        }
        
        // Remove event listeners
        if (typeof window !== 'undefined') {
            window.removeEventListener('beforeunload', this._handleBeforeUnload);
        }
        
        logger.info('DANIQUE resources cleaned up');
    }

    /**
     * Generate a unique task ID
     * @private
     * @returns {string} Unique task ID
     */
    _generateTaskId() {
        return 'task_' + Math.random().toString(36).substr(2, 9) + '_' + Date.now();
    }

    /**
     * Insert a task into the queue based on priority
     * @private
     * @param {Object} taskWrapper - Task wrapper object
     */
    _insertWithPriority(taskWrapper) {
        // Use binary search for faster insertion (O(log n) instead of O(n))
        let low = 0;
        let high = this.queue.length - 1;
        
        while (low <= high) {
            const mid = Math.floor((low + high) / 2);
            if (this.queue[mid].priority < taskWrapper.priority) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        
        this.queue.splice(low, 0, taskWrapper);
    }

    /**
     * Add task dependencies to the dependency graph
     * @private
     * @param {Object} taskWrapper - Task wrapper object
     */
    _addTaskDependencies(taskWrapper) {
        // Initialize dependencies for this task
        this.dependencyGraph.set(taskWrapper.id, {
            task: taskWrapper,
            dependsOn: new Set(taskWrapper.depends),
            dependents: new Set()
        });
        
        // Update the dependency graph with reverse dependencies
        taskWrapper.depends.forEach(depId => {
            if (this.dependencyGraph.has(depId)) {
                this.dependencyGraph.get(depId).dependents.add(taskWrapper.id);
            } else {
                // Create a placeholder for dependencies that don't exist yet
                this.dependencyGraph.set(depId, {
                    task: null,
                    dependsOn: new Set(),
                    dependents: new Set([taskWrapper.id])
                });
            }
        });
    }

    /**
     * Check if all dependencies for a task are complete
     * @private
     * @param {string} taskId - Task ID to check
     * @returns {boolean} True if all dependencies are complete
     */
    _areDependenciesMet(taskId) {
        if (!this.dependencyGraph.has(taskId)) {
            return true;
        }
        
        const { dependsOn } = this.dependencyGraph.get(taskId);
        return dependsOn.size === 0;
    }

    /**
     * Update dependencies when a task completes
     * @private
     * @param {string} taskId - ID of the completed task
     */
    _updateDependencies(taskId) {
        if (!this.dependencyGraph.has(taskId)) {
            return;
        }
        
        const { dependents } = this.dependencyGraph.get(taskId);
        
        // Process each dependent task
        dependents.forEach(depId => {
            if (this.dependencyGraph.has(depId)) {
                const depNode = this.dependencyGraph.get(depId);
                depNode.dependsOn.delete(taskId);
                
                // If all dependencies are met, add the task to the queue
                if (depNode.dependsOn.size === 0 && depNode.task) {
                    this._insertWithPriority(depNode.task);
                    // Use microtask queue for better performance
                    queueMicrotask(() => this._next());
                }
            }
        });
    }

    /**
     * Update task progress
     * @private
     * @param {string} taskId - ID of the task
     * @param {number} percent - Progress percentage (0-100)
     */
    _updateTaskProgress(taskId, percent) {
        if (!this.taskMap.has(taskId)) {
            return;
        }
        
        const task = this.taskMap.get(taskId);
        const validPercent = Math.max(0, Math.min(100, percent));
        task.progress = validPercent;
        
        // Trigger progress hook if defined
        if (typeof this.onTaskProgress === 'function') {
            try {
                this.onTaskProgress(taskId, validPercent, task.metadata);
            } catch (err) {
                logger.warn(`Error in onTaskProgress hook: ${err.message}`);
            }
        }
        
        // Update persistence if enabled
        if (this.persistTasks) {
            this._persistTasks();
        }
    }

    /**
     * Handle beforeunload event for graceful shutdown
     * @private
     * @param {Event} event - BeforeUnload event
     */
    _handleBeforeUnload(event) {
        // Persist tasks before unload if enabled
        if (this.persistTasks) {
            this._persistTasks();
        }
        
        // If tasks are running, show confirmation dialog
        if (this.running > 0) {
            const message = `${this.running} tasks still running. Are you sure you want to leave?`;
            event.returnValue = message;
            return message;
        }
    }

    /**
     * Check memory usage
     * @private
     * @returns {number} Memory usage percentage
     */
    _checkMemoryUsage() {
        // Check memory usage in browser environment
        if (typeof window !== 'undefined' && window.performance && window.performance.memory) {
            const memoryInfo = window.performance.memory;
            const usedPercent = (memoryInfo.usedJSHeapSize / memoryInfo.jsHeapSizeLimit) * 100;
            
            // Trigger warning if threshold exceeded
            if (usedPercent > this.memoryWarningThreshold && typeof this.onMemoryWarning === 'function') {
                this.onMemoryWarning(usedPercent);
            }
            
            return usedPercent;
        }
        
        // Check memory usage in Node.js environment
        if (typeof process !== 'undefined' && process.memoryUsage) {
            const memoryInfo = process.memoryUsage();
            const usedPercent = (memoryInfo.heapUsed / memoryInfo.heapTotal) * 100;
            
            // Trigger warning if threshold exceeded
            if (usedPercent > this.memoryWarningThreshold && typeof this.onMemoryWarning === 'function') {
                this.onMemoryWarning(usedPercent);
            }
            
            return usedPercent;
        }
        
        return 0; // Default if memory usage can't be determined
    }

    /**
     * Clean up completed tasks to free memory
     * @private
     */
    _cleanupCompletedTasks() {
        // Keep only the last 100 completed/failed tasks
        if (this.completedTasks.length > 100) {
            const tasksToRemove = this.completedTasks.slice(0, this.completedTasks.length - 100);
            tasksToRemove.forEach(taskId => {
                if (!this.dependencyGraph.has(taskId)) {
                    this.taskMap.delete(taskId);
                }
            });
            this.completedTasks = this.completedTasks.slice(-100);
        }
        
        if (this.failedTasks.length > 100) {
            const tasksToRemove = this.failedTasks.slice(0, this.failedTasks.length - 100);
            tasksToRemove.forEach(taskId => {
                if (!this.dependencyGraph.has(taskId)) {
                    this.taskMap.delete(taskId);
                }
            });
            this.failedTasks = this.failedTasks.slice(-100);
        }
    }

    /**
     * Persist tasks to storage
     * @private
     */
    _persistTasks() {
        if (!this.persistTasks) return;
        
        const serializedTasks = this._serializeTasks();
        
        try {
            if (this.storageType === 'localStorage' && typeof localStorage !== 'undefined') {
                localStorage.setItem(this.persistenceKey, serializedTasks);
            } else if (this.storageType === 'sessionStorage' && typeof sessionStorage !== 'undefined') {
                sessionStorage.setItem(this.persistenceKey, serializedTasks);
            }
            // Could add more storage types here (IndexedDB, etc.)
        } catch (err) {
            logger.warn(`Failed to persist tasks: ${err.message}`);
        }
    }

    /**
     * Load persisted tasks from storage
     * @private
     */
    _loadPersistedTasks() {
        if (!this.persistTasks) return;
        
        try {
            let serializedTasks = null;
            
            if (this.storageType === 'localStorage' && typeof localStorage !== 'undefined') {
                serializedTasks = localStorage.getItem(this.persistenceKey);
            } else if (this.storageType === 'sessionStorage' && typeof sessionStorage !== 'undefined') {
                serializedTasks = sessionStorage.getItem(this.persistenceKey);
            }
            
            if (serializedTasks) {
                this._deserializeTasks(serializedTasks);
                logger.info(`Loaded ${this.queue.length} persisted tasks`);
            }
        } catch (err) {
            logger.warn(`Failed to load persisted tasks: ${err.message}`);
        }
    }

    /**
     * Serialize tasks for persistence
     * @private
     * @returns {string} Serialized tasks
     */
    _serializeTasks() {
        // We can only serialize certain properties
        const serializableTasks = this.queue.map(task => ({
            id: task.id,
            priority: task.priority,
            timeout: task.timeout,
            retries: task.retries,
            retriesLeft: task.retriesLeft,
            jsonInput: task.jsonInput,
            jsonOutput: task.jsonOutput,
            args: task.args,
            depends: task.depends,
            metadata: task.metadata || {},
            status: task.status,
            progress: task.progress,
            createdAt: task.createdAt.toISOString(),
            // We can't serialize the function directly, so store its string representation
            fnString: task.fn.toString(),
            // We also can't store the promises directly
        }));
        
        return JSON.stringify({
            tasks: serializableTasks,
            completed: this.completedTasks,
            failed: this.failedTasks
        });
    }

    /**
     * Deserialize tasks from persistence
     * @private
     * @param {string} serializedData - Serialized task data
     */
    _deserializeTasks(serializedData) {
        try {
            const data = JSON.parse(serializedData);
            
            // Restore completed and failed task lists
            this.completedTasks = data.completed || [];
            this.failedTasks = data.failed || [];
            
            // Process each task
            data.tasks.forEach(taskData => {
                try {
                    // Convert function string back to function
                    // NOTE: This uses eval which has security implications
                    // In production, you might want a safer approach
                    let fn;
                    try {
                        fn = eval('(' + taskData.fnString + ')');
                    } catch (err) {
                        logger.warn(`Failed to deserialize task function: ${err.message}`);
                        // Create a placeholder function
                        fn = () => Promise.reject(new Error('Task function could not be restored'));
                    }
                    
                    // Create new promise
                    let taskResolve, taskReject;
                    const taskPromise = new Promise((resolve, reject) => {
                        taskResolve = resolve;
                        taskReject = reject;
                    });
                    
                    // Create task wrapper
                    const taskWrapper = {
                        fn,
                        priority: taskData.priority,
                        timeout: taskData.timeout,
                        retries: taskData.retries,
                        retriesLeft: taskData.retriesLeft,
                        id: taskData.id,
                        resolve: taskResolve,
                        reject: taskReject,
                        jsonInput: taskData.jsonInput,
                        jsonOutput: taskData.jsonOutput,
                        args: taskData.args,
                        depends: taskData.depends,
                        metadata: taskData.metadata,
                        status: taskData.status,
                        progress: taskData.progress,
                        createdAt: new Date(taskData.createdAt),
                        startedAt: null,
                        completedAt: null,
                        reportProgress: (percent) => {
                            this._updateTaskProgress(taskData.id, percent);
                        }
                    };
                    
                    // Add to taskMap
                    this.taskMap.set(taskData.id, taskWrapper);
                    
                    // Handle dependencies
                    if (taskWrapper.depends.length > 0) {
                        this._addTaskDependencies(taskWrapper);
                    } else {
                        // Add to queue if no dependencies
                        this._insertWithPriority(taskWrapper);
                    }
                } catch (err) {
                    logger.warn(`Failed to deserialize task: ${err.message}`);
                }
            });
        } catch (err) {
            logger.error(`Failed to parse serialized tasks: ${err.message}`);
        }
    }

    /**
     * Process the next task in the queue
     * @private
     */
    _next() {
        // Don't process if paused, stopped, or at concurrency limit
        if (this.isPaused || this.isStopped || this.isShuttingDown || 
            (this.running >= this.concurrency) || 
            (this.stopOnError && this.hasError) || 
            this.queue.length === 0) {
        
            // If queue is empty and no tasks are running, trigger onQueueEmpty
            if (this.queue.length === 0 && this.running === 0) {
                if (typeof this.onQueueEmpty === 'function') {
                    try {
                        this.onQueueEmpty();
                    } catch (err) {
                        logger.warn(`Error in onQueueEmpty hook: ${err.message}`);
                    }
                }
                
                // Also trigger onQueueDrained if defined
                if (typeof this.onQueueDrained === 'function') {
                    try {
                        this.onQueueDrained({
                            tasksCompleted: this.tasksCompleted,
                            tasksFailed: this.tasksFailed
                        });
                    } catch (err) {
                        logger.warn(`Error in onQueueDrained hook: ${err.message}`);
                    }
                }
            }
            
            return;
        }

        // Handle rate limiting
        if (this.rateLimitPerSecond > 0) {
            const now = Date.now();
            const elapsed = now - this.rateLimit.lastCheck;
            
            // Reset counter after a second
            if (elapsed > 1000) {
                this.rateLimit.count = 0;
                this.rateLimit.lastCheck = now;
            }
            
            // Check if we're above the rate limit
            if (this.rateLimit.count >= this.rateLimitPerSecond) {
                // Schedule next attempt after rate limit period
                setTimeout(() => this._next(), 1000 - elapsed);
                return;
            }
            
            // Increment counter
            this.rateLimit.count++;
        }

        // Get next task and start executing
        const taskWrapper = this.queue.shift();
        
        // Skip if task was cancelled
        if (taskWrapper.wasCancelled) {
            queueMicrotask(() => this._next());
            return;
        }
        
        this.running++;
        
        // Update task status
        taskWrapper.status = 'running';
        taskWrapper.startedAt = new Date();
        
        // Execute with microtask scheduling for better performance
        queueMicrotask(() => this._executeTask(taskWrapper));
    }

    /**
     * Execute a task with timeout handling and retries
     * @private
     * @param {Object} taskWrapper - Task wrapper object
     */
    _executeTask(taskWrapper) {
        const { fn, timeout, jsonInput, jsonOutput, args, id } = taskWrapper;
        
        // Trigger onTaskStart hook if defined
        if (typeof this.onTaskStart === 'function') {
            try {
                this.onTaskStart(taskWrapper);
            } catch (err) {
                logger.warn(`Error in onTaskStart hook: ${err.message}`);
            }
        }

        let timeoutId;
        
        // Create promise for task execution
        const executePromise = new Promise((resolve, reject) => {
            // Set timeout if specified
            if (timeout > 0) {
                timeoutId = setTimeout(() => {
                    reject(new Error(`Task timed out after ${timeout}ms`));
                }, timeout);
            }
            
            // Check if task was cancelled during setup
            if (taskWrapper.wasCancelled) {
                reject(new Error('Task was cancelled'));
                return;
            }
            
            try {
                // Process arguments if JSON input is requested
                const processedArgs = jsonInput ? 
                    args.map(arg => typeof arg === 'string' ? JSON.parse(arg) : arg) : 
                    args;
                
                // Add progress reporting function as the last argument
                const allArgs = [...processedArgs, taskWrapper.reportProgress];
                
                // Execute the task function
                const result = fn(...allArgs);
                
                // Handle promise-returning tasks
                if (result && typeof result.then === 'function') {
                    result
                        .then(value => {
                            // Process JSON output if requested
                            if (jsonOutput && value !== undefined) {
                                try {
                                    value = typeof value === 'string' ? value : JSON.stringify(value);
                                } catch (err) {
                                    reject(new Error(`Failed to stringify result: ${err.message}`));
                                    return;
                                }
                            }
                            resolve(value);
                        })
                        .catch(reject);
                } else {
                    // Handle synchronous tasks
                    if (jsonOutput && result !== undefined) {
                        try {
                            resolve(typeof result === 'string' ? result : JSON.stringify(result));
                        } catch (err) {
                            reject(new Error(`Failed to stringify result: ${err.message}`));
                        }
                    } else {
                        resolve(result);
                    }
                }
            } catch (err) {
                reject(err);
            }
        });

        // Handle task completion
        executePromise
            .then(result => {
                // Clear timeout if set
                if (timeoutId) clearTimeout(timeoutId);
                
                // Task succeeded
                this._handleTaskSuccess(taskWrapper, result);
            })
            .catch(err => {
                // Clear timeout if set
                if (timeoutId) clearTimeout(timeoutId);
                
                // Task failed
                this._handleTaskError(taskWrapper, err);
            });
    }

    /**
     * Handle successful task completion
     * @private
     * @param {Object} taskWrapper - Task wrapper object
     * @param {*} result - Task result
     */
    _handleTaskSuccess(taskWrapper, result) {
        // Update task status
        taskWrapper.status = 'completed';
        taskWrapper.progress = 100;
        taskWrapper.completedAt = new Date();
        this.tasksCompleted++;
        
        // Add to completed tasks list
        this.completedTasks.push(taskWrapper.id);
        
        // Trigger success hook if defined
        if (typeof this.onTaskSuccess === 'function') {
            try {
                this.onTaskSuccess(result, taskWrapper);
            } catch (err) {
                logger.warn(`Error in onTaskSuccess hook: ${err.message}`);
            }
        }
        
        // Resolve the task's promise
        taskWrapper.resolve(result);
        
        // Update dependencies
        this._updateDependencies(taskWrapper.id);
        
        // Update state and process next task
        this.running--;
        
        // Update persistence if enabled
        if (this.persistTasks) {
            this._persistTasks();
        }
        
        this._next();
        
        // If in sequential mode or we freed up a slot, schedule the next task
        if ((this.sequential && this.running === 0) || 
            (!this.sequential && this.running < this.concurrency)) {
            queueMicrotask(() => this._next());
        }
    }

    /**
     * Handle task error
     * @private
     * @param {Object} taskWrapper - Task wrapper object
     * @param {Error} error - Task error
     */
    _handleTaskError(taskWrapper, error) {
        // Set error flag if stopOnError is true
        if (this.stopOnError) {
            this.hasError = true;
        }
        
        // Check if we should retry
        if (taskWrapper.retriesLeft > 0) {
            logger.info(`Retrying task ${taskWrapper.id} (${taskWrapper.retriesLeft} retries left)...`);
            
            // Decrement retry counter
            taskWrapper.retriesLeft--;
            
            // Add back to the front of the queue with the same priority
            this.queue.unshift(taskWrapper);
            
            // Reset status to pending
            taskWrapper.status = 'pending';
        } else {
            // Update task status
            taskWrapper.status = 'failed';
            taskWrapper.completedAt = new Date();
            this.tasksFailed++;
            
            // Add to failed tasks list
            this.failedTasks.push(taskWrapper.id);
            
            // Trigger error hook if defined
            if (typeof this.onTaskError === 'function') {
                try {
                    this.onTaskError(error, taskWrapper);
                } catch (err) {
                    logger.warn(`Error in onTaskError hook: ${err.message}`);
                }
            }
            
            // Log error
            logger.error(`Task ${taskWrapper.id} failed: ${error.message}`);
            
            // Reject the task's promise
            taskWrapper.reject(error);
            
            // Update dependencies - failed tasks also release their dependents
            this._updateDependencies(taskWrapper.id);
        }
        
        // Update state and process next task
        this.running--;
        
        // Update persistence if enabled
        if (this.persistTasks) {
            this._persistTasks();
        }
        
        // If not stopping on errors or if it's the first error
        if (!this.stopOnError || !this.hasError) {
            this._next();
        } else if (this.stopOnError && this.hasError) {
            // If stopping on error and we hit an error, reject all pending tasks
            this.clear(true, new Error('Queue stopped due to error'));
        }
    }
}

/**
 * Logger for DANIQUE
 */
const logger = {
    /**
     * Log an error message in red
     * @param {string} message - The message to log
     */
    error: (message) => {
        console.log('%c DANIQUE ERROR: ' + message, 'color: #E53935; background: rgba(229, 57, 53, 0.1); padding: 2px 5px; border-left: 3px solid #E53935;');
    },

    /**
     * Log an info message in normal/gray
     * @param {string} message - The message to log
     */
    info: (message) => {
        console.log('%c DANIQUE INFO: ' + message, 'color: #607D8B; background: rgba(96, 125, 139, 0.1); padding: 2px 5px; border-left: 3px solid #607D8B;');
    },

    /**
     * Log a success message in green
     * @param {string} message - The message to log
     */
    success: (message) => {
        console.log('%c DANIQUE SUCCESS: ' + message, 'color: #43A047; background: rgba(67, 160, 71, 0.1); padding: 2px 5px; border-left: 3px solid #43A047;');
    },

    /**
     * Log a warning message in gold
     * @param {string} message - The message to log
     */
    warn: (message) => {
        console.log('%c DANIQUE WARNING: ' + message, 'color: #FFA000; background: rgba(255, 160, 0, 0.1); padding: 2px 5px; border-left: 3px solid #FFA000;');
    }
};

// Export for CommonJS and ESM compatibility
if (typeof module !== 'undefined' && module.exports) {
    module.exports = { DANIQUE, logger };
} else if (typeof define === 'function' && define.amd) {
    define(function() { return { DANIQUE, logger }; });
} else if (typeof window !== 'undefined') {
    window.DANIQUE = DANIQUE;
    window.DANIQUELogger = logger;
}
