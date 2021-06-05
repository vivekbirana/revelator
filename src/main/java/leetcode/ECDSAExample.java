package leetcode;

import java.math.BigInteger;
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

        KeyPairGenerator keyGen = KeyPairGenerator.getInstance("EC");

        keyGen.initialize(new ECGenParameterSpec("secp256r1"), new SecureRandom());

        KeyPair pair = keyGen.generateKeyPair();
        PrivateKey priv = pair.getPrivate();
        PublicKey pub = pair.getPublic();

        /*
         * Create a Signature object and initialize it with the private key
         */

        Signature ecdsa = Signature.getInstance("SHA256withECDSA");

        ecdsa.initSign(priv);

        String str = "This is string to sign";
        byte[] strByte = str.getBytes("UTF-8");
        ecdsa.update(strByte);

        /*
         * Now that all the data to be signed has been read in, generate a
         * signature for it
         */

        byte[] realSig = ecdsa.sign();
        System.out.println("Signature: " + new BigInteger(1, realSig).toString(16));

    }
}