import fs from "fs";

// --- PRICE CALCULATION ---
function calculateExecutionPrice(amount0, amount1, decimals0, decimals1) {
    // Convert string amounts to numbers and get absolute values
    const abs_amount0 = Math.abs(parseFloat(amount0));
    const abs_amount1 = Math.abs(parseFloat(amount1));

    // Skip if either amount is 0 (shouldn't happen in normal swaps)
    if (abs_amount0 === 0 || abs_amount1 === 0) {
        return null;
    }

    // The amounts from ethers.formatUnits are already normalized with decimals
    // But let's be explicit about the calculation:
    // For WBTC/USDC: 
    // - amount0 = WBTC (base currency, 8 decimals) - already normalized
    // - amount1 = USDC (quote currency, 6 decimals) - already normalized
    // Execution price = USDC per WBTC = abs(normalized_amount1) / abs(normalized_amount0)

    const executionPrice = abs_amount1 / abs_amount0;

    return executionPrice;
}

// Alternative function if amounts are NOT pre-normalized (raw blockchain values)
function calculateExecutionPriceFromRaw(rawAmount0, rawAmount1, decimals0, decimals1) {
    // Convert to absolute values
    const abs_raw0 = Math.abs(BigInt(rawAmount0));
    const abs_raw1 = Math.abs(BigInt(rawAmount1));

    if (abs_raw0 === 0n || abs_raw1 === 0n) {
        return null;
    }

    // Normalize with decimals
    const normalized_amount0 = Number(abs_raw0) / Math.pow(10, decimals0);
    const normalized_amount1 = Number(abs_raw1) / Math.pow(10, decimals1);

    // Calculate execution price = quote/base
    const executionPrice = normalized_amount1 / normalized_amount0;

    return executionPrice;
}

// --- MAIN ---
async function main() {
    try {
        console.log("📖 Reading existing swap data...");

        // Read the existing JSON file
        const rawData = fs.readFileSync("uniswap_wbtc_usdc_swaps.json", "utf8");
        const swaps = JSON.parse(rawData);

        console.log(`Found ${swaps.length} swaps to update`);

        // Update each swap with correct execution price
        let validPrices = 0;
        for (let i = 0; i < swaps.length; i++) {
            const swap = swaps[i];

            // Note: The amounts in the JSON are already decimal-normalized by ethers.formatUnits()
            // amount0 = WBTC amount (already divided by 10^8)
            // amount1 = USDC amount (already divided by 10^6)

            const executionPrice = calculateExecutionPrice(
                swap.amount0, 
                swap.amount1, 
                8, // WBTC decimals (for reference, already applied)
                6  // USDC decimals (for reference, already applied)
            );

            if (executionPrice !== null) {
                swap.executionPrice = executionPrice;
                validPrices++;

                // Add debug info for first few swaps
                if (i < 3) {
                    console.log(`Debug swap ${i + 1}:`);
                    console.log(`  WBTC amount (normalized): ${swap.amount0}`);
                    console.log(`  USDC amount (normalized): ${swap.amount1}`);
                    console.log(`  Execution price: ${executionPrice.toFixed(2)} USDC per WBTC`);
                }
            } else {
                swap.executionPrice = null;
                console.warn(`⚠️  Invalid amounts for swap at block ${swap.blockNumber}: amount0=${swap.amount0}, amount1=${swap.amount1}`);
            }

            // Remove the old incorrect price field and replace with executionPrice
            delete swap.price;
        }

        console.log(`✅ Updated ${validPrices} swaps with valid execution prices`);

        // Save the corrected data
        fs.writeFileSync("uniswap_wbtc_usdc_swaps_corrected.json", JSON.stringify(swaps, null, 2));
        console.log("💾 Saved corrected data to uniswap_wbtc_usdc_swaps_corrected.json");

        // Show some sample prices for verification
        const validSwaps = swaps.filter(s => s.executionPrice !== null);
        if (validSwaps.length > 0) {
            console.log("\n📊 Sample execution prices:");
            validSwaps.slice(0, 5).forEach(swap => {
                console.log(`Block ${swap.blockNumber}: ${swap.executionPrice.toFixed(2)} USDC per WBTC`);
                console.log(`  - WBTC: ${swap.amount0}, USDC: ${swap.amount1}`);
            });

            const prices = validSwaps.map(s => s.executionPrice);
            const avgPrice = prices.reduce((a, b) => a + b, 0) / prices.length;
            const minPrice = Math.min(...prices);
            const maxPrice = Math.max(...prices);

            console.log(`\n📈 Price Statistics:`);
            console.log(`Average: ${avgPrice.toFixed(2)} USDC per WBTC`);
            console.log(`Min: ${minPrice.toFixed(2)} USDC per WBTC`);
            console.log(`Max: ${maxPrice.toFixed(2)} USDC per WBTC`);
        }

    } catch (error) {
        if (error.code === 'ENOENT') {
            console.error("❌ File 'uniswap_wbtc_usdc_swaps.json' not found. Please run the original script first.");
        } else {
            console.error("❌ Error:", error);
        }
    }
}

main().catch(console.error);