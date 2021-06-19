package leetcode;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.security.spec.ECGenParameterSpec;

public class ECDSAExample {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        /*
         * Generate an ECDSA signature
         */

        /*
         * Generate a key pair
         */

        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("EC");

        keyPairGenerator.initialize(new ECGenParameterSpec("secp256r1"), new SecureRandom());

        final KeyPair keyPair = keyPairGenerator.generateKeyPair();
        final PrivateKey privateKey = keyPair.getPrivate();
        final PublicKey publicKey = keyPair.getPublic();


        final Signature signature = Signature.getInstance("SHA256withECDSA");

        signature.initSign(privateKey);

        final String str = "This is string to sign";
        byte[] strByte = str.getBytes(StandardCharsets.UTF_8);
        signature.update(strByte);

        byte[] realSig = signature.sign();
        System.out.println("Signature: " + new BigInteger(1, realSig).toString(16));

    }
}