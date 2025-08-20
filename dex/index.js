import { ethers } from "ethers";
import fs from "fs";
import path from "path";

// --- CONFIG ---
const RPC_URL = [
    "https://mainnet.infura.io/v3/56556dffb0ac4198bc99d7459f7db0ba",
    "https://ethereum-rpc.publicnode.com",
];

// Pool config (Uniswap V3 WBTC/USDC 0.3%)
const POOL = {
    address: "0x99ac8ca7087fa4a2a1fb6357269965a2014abc35",
    decimals0: 8,  // WBTC
    decimals1: 6,  // USDC
    creationBlock: 12376729
};

// Block range
const START_BLOCK = 20187849; // 18687850
const END_BLOCK = 21525889;
const CHUNK_SIZE = 50000;

// File paths
const OUTPUT_FILE = "uniswap_wbtc_usdc_swaps.json";
const PROGRESS_FILE = "uniswap_fetch_progress.json";

// Swap ABI
const POOL_ABI = [
    "event Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)"
];

// --- RPC MANAGEMENT ---
class RPCManager {
    constructor(rpcUrls) {
        this.rpcUrls = rpcUrls;
        this.currentIndex = 0;
        this.failedRpcs = new Set();
        this.provider = null;
        this.lastFailResetTime = Date.now();
        this.failResetInterval = 10 * 60 * 1000; // Reset failed RPCs every 10 minutes
        // Explicitly define network to avoid auto-detection issues
        this.network = {
            name: 'mainnet',
            chainId: 1
        };
    }

    async getWorkingProvider() {
        // Reset failed RPCs periodically
        if (Date.now() - this.lastFailResetTime > this.failResetInterval) {
            console.log("🔄 Resetting failed RPC list");
            this.failedRpcs.clear();
            this.lastFailResetTime = Date.now();
        }

        // Try to get a working provider
        let attempts = 0;
        while (attempts < this.rpcUrls.length) {
            const rpcUrl = this.rpcUrls[this.currentIndex];
            
            if (!this.failedRpcs.has(rpcUrl)) {
                try {
                    if (!this.provider || this.provider._getConnection().url !== rpcUrl) {
                        console.log(`🔗 Connecting to RPC: ${rpcUrl}`);
                        // Specify network explicitly to avoid auto-detection
                        this.provider = new ethers.JsonRpcProvider(rpcUrl, this.network);
                    }
                    
                    // Test the connection with a simple call
                    const blockNumber = await this.provider.getBlockNumber();
                    console.log(`✅ RPC ${rpcUrl} is working (latest block: ${blockNumber})`);
                    return this.provider;
                    
                } catch (err) {
                    console.log(`❌ RPC ${rpcUrl} failed: ${err.message}`);
                    this.failedRpcs.add(rpcUrl);
                    this.moveToNextRpc();
                }
            } else {
                this.moveToNextRpc();
            }
            
            attempts++;
        }
        
        throw new Error("All RPCs failed! Waiting before retry...");
    }

    moveToNextRpc() {
        this.currentIndex = (this.currentIndex + 1) % this.rpcUrls.length;
    }

    async executeWithRetry(operation, maxRetries = 3) {
        for (let attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                const provider = await this.getWorkingProvider();
                return await operation(provider);
            } catch (err) {
                console.log(`⚠️  Attempt ${attempt}/${maxRetries} failed: ${err.message}`);
                
                if (attempt === maxRetries) {
                    if (err.message.includes("All RPCs failed")) {
                        console.log("😴 All RPCs failed, waiting 30 seconds before retry...");
                        await new Promise(res => setTimeout(res, 30000));
                        return this.executeWithRetry(operation, maxRetries); // Recursive retry
                    }
                    throw err;
                }
                
                // Mark current RPC as failed and try next
                if (this.provider) {
                    this.failedRpcs.add(this.provider._getConnection().url);
                }
                this.moveToNextRpc();
                
                // Wait before retry
                await new Promise(res => setTimeout(res, 2000 * attempt));
            }
        }
    }
}

// --- PRICE CONVERSION ---
function sqrtPriceX96ToPrice(sqrtPriceX96, decimals0, decimals1) {
    const sqrt = BigInt(sqrtPriceX96.toString());
    const Q96 = BigInt(2) ** BigInt(96);
    const ratio = (sqrt * sqrt * BigInt(10) ** BigInt(decimals0)) /
                  (Q96 * Q96 * BigInt(10) ** BigInt(decimals1));
    return Number(ratio) / 1e0; // already adjusted for decimals
}

// --- FILE OPERATIONS ---
function getExistingSwapCount() {
    try {
        if (fs.existsSync(OUTPUT_FILE)) {
            const data = fs.readFileSync(OUTPUT_FILE, 'utf8').trim();
            if (data.length === 0 || data === '[]') {
                return 0;
            }
            // Count entries by counting commas and adding 1 (rough estimate)
            const matches = data.match(/},/g);
            const count = matches ? matches.length + 1 : 1;
            console.log(`📁 Found existing file with approximately ${count} swaps`);
            return count;
        }
    } catch (err) {
        console.error(`Error reading existing data: ${err.message}`);
        // Create backup of corrupted file
        if (fs.existsSync(OUTPUT_FILE)) {
            const backupName = `${OUTPUT_FILE}.backup.${Date.now()}`;
            fs.copyFileSync(OUTPUT_FILE, backupName);
            console.log(`🔄 Corrupted file backed up as ${backupName}`);
        }
    }
    return 0;
}

function loadProgress() {
    try {
        if (fs.existsSync(PROGRESS_FILE)) {
            const data = fs.readFileSync(PROGRESS_FILE, 'utf8');
            return JSON.parse(data);
        }
    } catch (err) {
        console.error(`Error loading progress: ${err.message}`);
    }
    return { lastProcessedBlock: START_BLOCK - 1, totalSwaps: 0 };
}

function saveProgress(lastProcessedBlock, totalSwaps) {
    const progress = {
        lastProcessedBlock,
        totalSwaps,
        timestamp: new Date().toISOString(),
        startBlock: START_BLOCK,
        endBlock: END_BLOCK
    };
    
    try {
        fs.writeFileSync(PROGRESS_FILE, JSON.stringify(progress, null, 2));
    } catch (err) {
        console.error(`Error saving progress: ${err.message}`);
    }
}

function appendSwapsToFile(newSwaps) {
    if (newSwaps.length === 0) return true;
    
    try {
        const tempFile = `${OUTPUT_FILE}.temp`;
        
        if (!fs.existsSync(OUTPUT_FILE)) {
            // First time - create new file with JSON array
            fs.writeFileSync(OUTPUT_FILE, JSON.stringify(newSwaps, null, 2));
            console.log(`📄 Created new file with ${newSwaps.length} swaps`);
            return true;
        }
        
        // Read existing file
        let existingData = fs.readFileSync(OUTPUT_FILE, 'utf8').trim();
        
        if (existingData === '' || existingData === '[]') {
            // Empty file - write as new array
            fs.writeFileSync(OUTPUT_FILE, JSON.stringify(newSwaps, null, 2));
            console.log(`📄 Wrote to empty file: ${newSwaps.length} swaps`);
            return true;
        }
        
        // Remove the closing bracket and newlines from existing data
        existingData = existingData.replace(/\s*\]$/, '');
        
        // Create new data string
        let newDataString = '';
        if (existingData.trim().endsWith('[')) {
            // File only has opening bracket (empty array)
            newDataString = newSwaps.map(swap => '  ' + JSON.stringify(swap)).join(',\n');
        } else {
            // File has existing data - add comma and new entries
            newDataString = ',\n' + newSwaps.map(swap => '  ' + JSON.stringify(swap)).join(',\n');
        }
        
        // Write to temp file first
        fs.writeFileSync(tempFile, existingData + newDataString + '\n]');
        
        // Validate the JSON by parsing it
        const testData = fs.readFileSync(tempFile, 'utf8');
        JSON.parse(testData); // This will throw if invalid
        
        // If validation passes, replace the original file
        fs.renameSync(tempFile, OUTPUT_FILE);
        
        console.log(`📄 Successfully appended ${newSwaps.length} swaps to file`);
        return true;
        
    } catch (err) {
        console.error(`Error appending swaps to file: ${err.message}`);
        
        // Clean up temp file if it exists
        const tempFile = `${OUTPUT_FILE}.temp`;
        if (fs.existsSync(tempFile)) {
            fs.unlinkSync(tempFile);
        }
        
        return false;
    }
}

// --- MAIN ---
async function main() {
    console.log(`🚀 Fetching swap data for ${POOL.address} from ${START_BLOCK} to ${END_BLOCK}`);
    
    // Initialize RPC manager
    const rpcManager = new RPCManager(RPC_URL);
    
    // Load progress and check existing data
    const progress = loadProgress();
    const existingSwapCount = getExistingSwapCount();
    
    console.log(`📊 Resume from block ${progress.lastProcessedBlock + 1}`);
    console.log(`📈 Current total swaps in file: ${existingSwapCount}`);

    let totalNewSwaps = 0;
    let startFromBlock = Math.max(progress.lastProcessedBlock + 1, START_BLOCK);

    for (let fromBlock = startFromBlock; fromBlock <= END_BLOCK; fromBlock += CHUNK_SIZE) {
        const toBlock = Math.min(fromBlock + CHUNK_SIZE - 1, END_BLOCK);
        const progressPercent = ((fromBlock - START_BLOCK) / (END_BLOCK - START_BLOCK) * 100).toFixed(1);
        
        console.log(`\n📊 [${progressPercent}%] Processing blocks ${fromBlock} to ${toBlock}...`);

        try {
            // Fetch logs with RPC failover
            const logs = await rpcManager.executeWithRetry(async (provider) => {
                return await provider.getLogs({
                    address: POOL.address,
                    topics: [ethers.id("Swap(address,address,int256,int256,uint160,uint128,int24)")],
                    fromBlock,
                    toBlock
                });
            });

            console.log(`   Found ${logs.length} swap events in this chunk`);

            let chunkSwaps = [];
            for (const log of logs) {
                try {
                    // Parse log with RPC failover for getting block data
                    const swapData = await rpcManager.executeWithRetry(async (provider) => {
                        const pool = new ethers.Contract(POOL.address, POOL_ABI, provider);
                        const parsed = pool.interface.parseLog(log);
                        const block = await provider.getBlock(log.blockNumber);
                        const price = sqrtPriceX96ToPrice(parsed.args.sqrtPriceX96, POOL.decimals0, POOL.decimals1);

                        return {
                            timestamp: block.timestamp,
                            datetime: new Date(block.timestamp * 1000).toISOString(),
                            blockNumber: log.blockNumber,
                            txHash: log.transactionHash,
                            sender: parsed.args.sender,
                            recipient: parsed.args.recipient,
                            amount0: ethers.formatUnits(parsed.args.amount0, POOL.decimals0),
                            amount1: ethers.formatUnits(parsed.args.amount1, POOL.decimals1),
                            price,
                            tick: parsed.args.tick.toString()
                        };
                    });

                    chunkSwaps.push(swapData);
                } catch (err) {
                    console.error(`Error parsing log at block ${log.blockNumber}:`, err.message);
                }
            }

            // Append chunk data to file immediately
            if (appendSwapsToFile(chunkSwaps)) {
                totalNewSwaps += chunkSwaps.length;
                const currentTotal = existingSwapCount + totalNewSwaps;
                
                saveProgress(toBlock, currentTotal);
                console.log(`   ✅ Appended ${chunkSwaps.length} swaps to file. Total in file: ${currentTotal}`);
            } else {
                console.log(`   ❌ Failed to append chunk data to file!`);
                break; // Stop processing if we can't save
            }

            // Rate limiting
            await new Promise(res => setTimeout(res, 100));
            
        } catch (err) {
            console.error(`❌ Error fetching blocks ${fromBlock}-${toBlock}:`, err.message);
            console.log(`🔄 Will continue from block ${toBlock + 1} on next run`);
            
            // Save current progress even on error
            saveProgress(fromBlock - 1, existingSwapCount + totalNewSwaps);
            
            // Wait a bit longer before retrying
            await new Promise(res => setTimeout(res, 5000));
        }
    }

    console.log(`\n🎉 COMPLETED!`);
    console.log(`📊 Total swaps in file: ${existingSwapCount + totalNewSwaps}`);
    console.log(`📈 New swaps added this run: ${totalNewSwaps}`);
    console.log(`💾 Data saved to: ${OUTPUT_FILE}`);
    
    // Clean up progress file when complete
    if (fs.existsSync(PROGRESS_FILE)) {
        fs.unlinkSync(PROGRESS_FILE);
        console.log(`🧹 Cleaned up progress file`);
    }
}

// Handle process interruption gracefully
process.on('SIGINT', () => {
    console.log('\n⚠️  Process interrupted. Progress has been saved.');
    console.log('📝 Run the script again to resume from where it left off.');
    process.exit(0);
});

process.on('uncaughtException', (err) => {
    console.error('💥 Uncaught exception:', err);
    console.log('📝 Progress has been saved. Run the script again to resume.');
    process.exit(1);
});

main().catch((err) => {
    console.error('💥 Main process error:', err);
    console.log('📝 Run the script again to resume from last saved progress.');
    process.exit(1);
});