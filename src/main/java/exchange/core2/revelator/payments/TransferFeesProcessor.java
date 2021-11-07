package exchange.core2.revelator.payments;

import org.eclipse.collections.impl.map.mutable.primitive.ShortLongHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.ShortObjectHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TransferFeesProcessor {

    private static final Logger log = LoggerFactory.getLogger(TransferFeesProcessor.class);

    private final CurrencyRateProcessor currencyRateProcessor;

    private double feeK = 0.0;
    private final ShortObjectHashMap<FeeConfig> currencyFees = new ShortObjectHashMap<>();
    private final ShortLongHashMap treasures = new ShortLongHashMap();

    public TransferFeesProcessor(CurrencyRateProcessor currencyRateProcessor) {
        this.currencyRateProcessor = currencyRateProcessor;
    }

    public long processDstExact(long amountMsg, short currencySrc, short currencyDst) {
        // TODO handle errors

        // calculate source amount

        final long amountSrcRaw;

        if (currencyDst == currencySrc) {
            amountSrcRaw = amountMsg;
        } else {
            amountSrcRaw = currencyRateProcessor.convertRate(amountMsg, currencyDst, currencySrc);

            // update treasures for currency conversion
            addTreasure(currencyDst, -amountMsg);
            addTreasure(currencySrc, amountSrcRaw);
        }

        // apply fee on source amount
        final long fee = calculateFee(amountSrcRaw, currencySrc);

        addTreasure(currencySrc, fee);

        return amountSrcRaw + fee;
    }

    public void revertDstExact(long amountMsg, short currencySrc, short currencyDst) {

        final long amountSrcRaw;

        if (currencyDst == currencySrc) {
            amountSrcRaw = amountMsg;
        } else {
            amountSrcRaw = currencyRateProcessor.convertRate(amountMsg, currencyDst, currencySrc);

            // revert treasures for currency conversion
            addTreasure(currencyDst, amountMsg);
            addTreasure(currencySrc, -amountSrcRaw);
        }

        // revert fee
        final long fee = calculateFee(amountSrcRaw, currencySrc);

        addTreasure(currencySrc, -fee);
    }


    public long processSrcExact(long amountMsg, short currencySrc, short currencyDst) {

        // calculate source fee
        final long feeSrc = calculateFee(amountMsg, currencySrc);

        final long amountSrcAfterFee = amountMsg - feeSrc;

        if (feeSrc >= amountMsg) {
            log.debug("fee {} >= amount {}", feeSrc, amountMsg);
            return -1;
        }

        final long amountDst;

        if (currencyDst == currencySrc) {
            amountDst = amountSrcAfterFee;
            addTreasure(currencySrc, feeSrc);

        } else {

            amountDst = currencyRateProcessor.convertRate(amountSrcAfterFee, currencySrc, currencyDst);

            // too small amount after conversion
            if (amountDst <= 0) {
                log.debug("too small amount after conversion - amountDst={} (currencySrc={} currencyDst={} amountSrcAfterFee={})", amountDst, currencySrc, currencyDst, amountSrcAfterFee);
                return -1;
            }

            addTreasure(currencyDst, -amountDst);
            addTreasure(currencySrc, amountMsg);
        }

        if (amountDst <= 0) {
            throw new IllegalStateException("amountMsg=" + amountMsg + " feeSrc=" + feeSrc + " amountSrcAfterFee=" + amountSrcAfterFee + " amountDst=" + amountDst);
        }

        return amountDst;
    }

    public void revertSrcExact(long amountMsg, short currencySrc, short currencyDst) {

        // calculate source fee
        final long feeSrc = calculateFee(amountMsg, currencySrc);

        final long amountSrcAfterFee = amountMsg - feeSrc;

        if (currencyDst == currencySrc) {
            addTreasure(currencySrc, -feeSrc);
        } else {
            final long amountDst = currencyRateProcessor.convertRate(amountSrcAfterFee, currencySrc, currencyDst);
            addTreasure(currencyDst, amountDst);
            addTreasure(currencySrc, -amountMsg);
        }
    }

    private long calculateFee(final long amount, final short currency) {

        final FeeConfig feeConfig = currencyFees.get(currency);

        if (feeConfig == null) {
            log.debug("no fee configuration for currency {}", currency);

            throw new IllegalStateException("no fee configuration for currency " + currency);
            //return -1;
        }

//        final long feeRaw = ArithmeticUtils.reciprocalMul(amount, feeOrderBefore, feeMultiplier, feeOrderAfter);


        final long feeRaw = ArithmeticUtils.multiplyByRate(amount, feeK);

        final long feeLimited = Math.max(feeConfig.minFee, Math.min(feeConfig.maxFee, feeRaw));

        return feeLimited;
    }


    public void addTreasure(short currency, long amount) {
        treasures.addToValue(currency, amount);
    }


    public void revertConversion(long amount, short currency) {
        treasures.addToValue(currency, -amount);
    }

    public void setFeeK(final double feeK) {

        this.feeK = feeK;
    }

    public void putFeeConfig(final short currency,
                             final long minFee,
                             final long maxFee) {

        currencyFees.put(currency, new FeeConfig(minFee, maxFee));
    }

    public void updateCurrencyRate(short currencyFrom, short currencyTo, double rate) {
        currencyRateProcessor.updateRate(currencyFrom, currencyTo, rate);
    }


    public record FeeConfig(long minFee, long maxFee) {
    }


}
