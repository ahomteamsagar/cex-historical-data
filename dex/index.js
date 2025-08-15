import { ethers } from "ethers";
import fs from "fs";

// --- CONFIG ---
const RPC_URL = "https://ethereum-rpc.publicnode.com";

// Pool config (Uniswap V3 WBTC/USDC 0.3%)
const POOL = {
    address: "0x99ac8ca7087fa4a2a1fb6357269965a2014abc35",
    decimals0: 8,  // WBTC
    decimals1: 6,  // USDC
    creationBlock: 12376729
};

// Block range
const START_BLOCK = 12376729; // ~ Jan 1, 2025
const END_BLOCK = 12376729 + 10000;   // ~ 1 month
const CHUNK_SIZE = 1000;

// Swap ABI
const POOL_ABI = [
    "event Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)"
];

// --- PRICE CONVERSION ---
function sqrtPriceX96ToPrice(sqrtPriceX96, decimals0, decimals1) {
    const sqrt = BigInt(sqrtPriceX96.toString());
    const Q96 = BigInt(2) ** BigInt(96);
    const ratio = (sqrt * sqrt * BigInt(10) ** BigInt(decimals0)) /
                  (Q96 * Q96 * BigInt(10) ** BigInt(decimals1));
    return Number(ratio) / 1e0; // already adjusted for decimals
}

// --- MAIN ---
async function main() {
    console.log(`🚀 Fetching swap data for ${POOL.address} from ${START_BLOCK} to ${END_BLOCK}`);
    
    const provider = new ethers.JsonRpcProvider(RPC_URL);
    const pool = new ethers.Contract(POOL.address, POOL_ABI, provider);

    let swaps = [];
    let totalLogs = 0;

    for (let fromBlock = START_BLOCK; fromBlock <= END_BLOCK; fromBlock += CHUNK_SIZE) {
        const toBlock = Math.min(fromBlock + CHUNK_SIZE - 1, END_BLOCK);
        console.log(`📊 Fetching blocks ${fromBlock} to ${toBlock}...`);

        try {
            const logs = await provider.getLogs({
                address: POOL.address,
                topics: [ethers.id("Swap(address,address,int256,int256,uint160,uint128,int24)")],
                fromBlock,
                toBlock
            });

            totalLogs += logs.length;
            console.log(`   Found ${logs.length} swap events`);

            for (const log of logs) {
                try {
                    const parsed = pool.interface.parseLog(log);
                    const block = await provider.getBlock(log.blockNumber);
                    const price = sqrtPriceX96ToPrice(parsed.args.sqrtPriceX96, POOL.decimals0, POOL.decimals1);

                    swaps.push({
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
                    });
                } catch (err) {
                    console.error(`Error parsing log:`, err);
                }
            }

            await new Promise(res => setTimeout(res, 100));
        } catch (err) {
            console.error(`Error fetching ${fromBlock}-${toBlock}:`, err);
        }
    }

    console.log(`✅ Total parsed swaps: ${swaps.length} (from ${totalLogs} logs)`);

    if (swaps.length > 0) {
        fs.writeFileSync("uniswap_wbtc_usdc_swaps.json", JSON.stringify(swaps, null, 2));
        console.log(`💾 Saved to uniswap_wbtc_usdc_swaps.json`);
    } else {
        console.log("❌ No swaps found in this block range");
    }
}

main().catch(console.error);
